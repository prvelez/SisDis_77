import math
import socket
import time
import struct
import threading
import queue
from decimal import Decimal, InvalidOperation

# =====================================================================
# CONFIGURACIÓN (Ajustar IP de VPN si es necesario)
# =====================================================================
VPN_IP = '25.17.234.244'  # Cambiar a la IP de Hamachi/ZeroTier del servidor
SERVER_IP = '0.0.0.0'
UNICAST_PORT = 5008
MULTICAST_PORT = 5009
MULTICAST_GROUP = '239.255.0.1'

# =====================================================================
# ESTADO GLOBAL Y LOCKS
# =====================================================================
precio_actual = Decimal('20.00')
mejor_postor = 'Ninguno'
version_estado = 0

estado_lock = threading.Lock()
clientes_lock = threading.Lock() # Lock exclusivo para el diccionario
print_lock = threading.Lock()
stop_event = threading.Event()   # Para cierre ordenado

clientes = {} 
# Memoria idempotente: (client_id, seq) -> (respuesta, timestamp)
pujas_procesadas = {}

def log(mensaje):
    with print_lock:
        print(mensaje)

def estado_serializado(version, precio, postor):
    return f'ESTADO_SUBASTA|{version}|{precio:.2f}|{postor}'

# =====================================================================
# TRABAJADOR CONCURRENTE
# =====================================================================
def trabajador_pujas(cola, sock_uni, sock_multi):
    global precio_actual, mejor_postor, version_estado
    while not stop_event.is_set():
        try:
            item = cola.get(timeout=1.0)
            if item is None: # Señal de apagado
                break
            data, addr = item
        except queue.Empty:
            continue

        try:
            mensaje = data.decode('utf-8').strip()
            partes = mensaje.split(';')
            
            if len(partes) != 5 or partes[0] != 'PUJA':
                raise ValueError('Formato inválido. Use: PUJA;id;seq;nombre;monto')
            
            client_id, seq, nombre, cantidad = partes[1], partes[2], partes[3].strip(), partes[4].strip()
            clave_idempotencia = (client_id, seq)
            
            respuesta_uni = None
            respuesta_multi = None
            
            with estado_lock:
                # 1. Verificar idempotencia
                if clave_idempotencia in pujas_procesadas:
                    respuesta_uni = pujas_procesadas[clave_idempotencia][0]
                else:
                    # 2. Validar nombre
                    if not nombre or len(nombre) > 40 or any(c in nombre for c in '|;:\n\r'):
                        raise ValueError('Nombre inválido')
                    
                    # 3. Validar monto (Finito, positivo, max 2 decimales, limite)
                    try:
                        oferta = Decimal(cantidad)
                        if not oferta.is_finite() or oferta <= 0:
                            raise ValueError('Monto debe ser positivo y finito')
                        if oferta.as_tuple().exponent < -2:
                            raise ValueError('Máximo 2 decimales permitidos')
                        if oferta > Decimal('999999999.99'):
                            raise ValueError('Monto excede el límite permitido')
                    except InvalidOperation:
                        raise ValueError('Monto numérico inválido')
                    
                    # 4. Lógica de subasta
                    if oferta <= precio_actual:
                        respuesta_uni = f'RECHAZADA|{seq}|Tu oferta ${oferta:.2f} debe superar ${precio_actual:.2f}'
                        log(f'[RECHAZADA] {nombre} (${oferta:.2f})')
                    else:
                        precio_actual = oferta
                        mejor_postor = nombre
                        version_estado += 1
                        
                        respuesta_uni = f'ACEPTADA|{seq}|Tu oferta de ${oferta:.2f} fue aceptada'
                        respuesta_multi = estado_serializado(version_estado, precio_actual, mejor_postor)
                        log(f'\n[ACEPTADA] {nombre} (${oferta:.2f}) | Versión {version_estado}')
                    
                    # Guardar con timestamp para limpieza
                    pujas_procesadas[clave_idempotencia] = (respuesta_uni, time.monotonic())

            # Red fuera del Lock
            sock_uni.sendto(respuesta_uni.encode('utf-8'), addr)
            if respuesta_multi:
                sock_multi.sendto(respuesta_multi.encode('utf-8'), (MULTICAST_GROUP, MULTICAST_PORT))

        except (UnicodeError, ValueError, IndexError) as exc:
            # Si falla, intentamos extraer el seq para avisar al cliente
            try:
                seq_err = mensaje.split(';')[2] if ';' in mensaje else '0'
            except IndexError:
                seq_err = '0'
            sock_uni.sendto(f'ERROR|{seq_err}|{exc}'.encode('utf-8'), addr)
            log(f'[ERROR] {addr}: {exc}')
        finally:
            cola.task_done()

# =====================================================================
# MANTENIMIENTO (Limpieza de clientes y memoria)
# =====================================================================
def tarea_mantenimiento():
    while not stop_event.is_set():
        time.sleep(10)
        ahora = time.monotonic()
        
        # Limpiar clientes inactivos
        with clientes_lock:
            inactivos = [c for c, t in clientes.items() if ahora - t > 30]
            for c in inactivos:
                del clientes[c]
                
        # Limpiar memoria de idempotencia (mayor a 5 minutos)
        with estado_lock:
            claves_viejas = [k for k, v in pujas_procesadas.items() if ahora - v[1] > 300]
            for k in claves_viejas:
                del pujas_procesadas[k]

# =====================================================================
# SERVIDOR PRINCIPAL
# =====================================================================
def main():
    sock_uni = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock_uni.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock_uni.bind((SERVER_IP, UNICAST_PORT))

    sock_multi = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock_multi.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 2)
    
    if VPN_IP != '0.0.0.0':
        sock_multi.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, socket.inet_aton(VPN_IP))

    log(f'[SERVIDOR] Unicast en {UNICAST_PORT} | Multicast en {MULTICAST_GROUP}:{MULTICAST_PORT}')
    
    cola = queue.Queue(maxsize=1000) # Cola con límite
    
    # Lanzar trabajadores
    workers = []
    for _ in range(4):
        t = threading.Thread(target=trabajador_pujas, args=(cola, sock_uni, sock_multi), daemon=True)
        t.start()
        workers.append(t)
        
    threading.Thread(target=tarea_mantenimiento, daemon=True).start()

    try:
        while True:
            data, addr = sock_uni.recvfrom(2048)
            mensaje = data.decode('utf-8').strip()
            
            # ---------------------------------------------------------
            # CORRECCIÓN: Interceptar ESTADO antes de la cola
            # ---------------------------------------------------------
            with clientes_lock:
                clientes[addr] = time.monotonic()
                
            if mensaje == 'ESTADO':
                with estado_lock:
                    resp = estado_serializado(version_estado, precio_actual, mejor_postor)
                sock_uni.sendto(resp.encode('utf-8'), addr)
                continue
            
            # Enviar pujas a los trabajadores
            try:
                cola.put((data, addr), block=False)
            except queue.Full:
                sock_uni.sendto(b'ERROR|0|Servidor ocupado, intente luego', addr)
                
    except KeyboardInterrupt:
        log('\n[SERVIDOR] Deteniendo...')
        stop_event.set()
        # Enviar centinelas para despertar a los hilos
        for _ in range(4):
            cola.put(None)
        for w in workers:
            w.join(timeout=2)
        sock_uni.close()
        sock_multi.close()

if __name__ == '__main__':
    main()