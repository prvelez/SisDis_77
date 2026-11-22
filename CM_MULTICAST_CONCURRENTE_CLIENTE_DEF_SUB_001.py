import socket
import sys
import time
import struct
import threading
import uuid
from decimal import Decimal, InvalidOperation

# =====================================================================
# CONFIGURACIÓN
# =====================================================================
SERVER_IP = input("IP del servidor (Enter para 127.0.0.1): ").strip() or "127.0.0.1"
VPN_IP = '25.32.175.96'  # Cambiar a tu IP de Hamachi/ZeroTier si el multicast falla

UNICAST_PORT = 5008
MULTICAST_PORT = 5009
MULTICAST_GROUP = '239.255.0.1'

nombre = input("Nombre del postor: ").strip().replace(";", "") or "Anonimo"
client_id = str(uuid.uuid4())[:8]

ultima_version = -1
version_lock = threading.Lock()
print_lock = threading.Lock()
stop_event = threading.Event()

# Control de respuestas para los reintentos
respuesta_evento = threading.Event()
ultima_respuesta = None

def log(mensaje, end="\n", flush=False):
    with print_lock:
        print(mensaje, end=end, flush=flush)

def procesar_estado(texto, origen):
    global ultima_version
    partes = texto.split('|')
    if len(partes) != 4: return
    
    try:
        version = int(partes[1])
        precio = partes[2]
        postor = partes[3]
    except ValueError:
        return

    with version_lock:
        if version > ultima_version:
            if ultima_version != -1 and version > ultima_version + 1:
                log(f"\n[!] ALERTA: Posible pérdida de datagrama (v{ultima_version} -> v{version})")
            
            ultima_version = version
            log(f"\n[{origen}] 🔔 Precio: ${precio}, Mejor postor: {postor}")
            log(f"[{nombre}] Tu puja: ", end="", flush=True)

# =====================================================================
# HILOS DE RECEPCIÓN
# =====================================================================
def hilo_multicast():
    sock_multi = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock_multi.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock_multi.bind(('', MULTICAST_PORT))
    
    mreq = struct.pack("4s4s", socket.inet_aton(MULTICAST_GROUP), socket.inet_aton(VPN_IP))
    sock_multi.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
    sock_multi.settimeout(1.0)

    try:
        while not stop_event.is_set():
            try:
                data, _ = sock_multi.recvfrom(1024)
                texto = data.decode('utf-8')
                if texto.startswith('ESTADO_SUBASTA'):
                    procesar_estado(texto, "MULTICAST")
            except socket.timeout:
                continue
            except Exception as exc:
                log(f"[ERROR MULTICAST] {exc}")
    finally:
        sock_multi.setsockopt(socket.IPPROTO_IP, socket.IP_DROP_MEMBERSHIP, mreq)
        sock_multi.close()

def hilo_unicast(sock):
    global ultima_respuesta
    sock.settimeout(1.0)
    try:
        while not stop_event.is_set():
            try:
                data, _ = sock.recvfrom(1024)
                texto = data.decode('utf-8')
                
                if texto.startswith('ESTADO_SUBASTA'):
                    procesar_estado(texto, "ESTADO INICIAL")
                elif texto.startswith(('ACEPTADA', 'RECHAZADA', 'ERROR')):
                    ultima_respuesta = texto
                    respuesta_evento.set() # Despierta al hilo principal
                    log(f"\n[SERVIDOR] {texto}")
                    log(f"[{nombre}] Tu puja: ", end="", flush=True)
            except socket.timeout:
                continue
            except Exception as exc:
                log(f"[ERROR UNICAST] {exc}")
    finally:
        sock.close()

# =====================================================================
# PRINCIPAL
# =====================================================================
if __name__ == '__main__':
    sock_uni = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    destino = (SERVER_IP, UNICAST_PORT)

    h1 = threading.Thread(target=hilo_multicast, daemon=True)
    h2 = threading.Thread(target=hilo_unicast, args=(sock_uni,), daemon=True)
    h1.start()
    h2.start()

    log(f"\nConectado como {nombre} (ID: {client_id}). Escribe tu puja o 'salir'.")
    sock_uni.sendto(b'ESTADO', destino)
    time.sleep(0.5)

    contador_pujas = 0
    try:
        while True:
            entrada = input(f"[{nombre}] Tu puja: ").strip()
            
            if entrada.lower() == 'salir':
                break
            if not entrada:
                continue
            
            # Validación local estricta
            try:
                oferta = Decimal(entrada)
                if not oferta.is_finite() or oferta <= 0:
                    raise ValueError("El monto debe ser positivo")
                if oferta.as_tuple().exponent < -2:
                    raise ValueError("Máximo 2 decimales permitidos")
                if oferta > Decimal('999999999.99'):
                    raise ValueError("Monto excede el límite")
            except (InvalidOperation, ValueError) as e:
                log(f"  [!] Error: {e}")
                continue

            contador_pujas += 1
            mensaje = f"PUJA;{client_id};{contador_pujas};{nombre};{entrada}"
            
            # Mecanismo de Reintento (Hasta 3 veces)
            respuesta_evento.clear()
            exito = False
            
            for intento in range(1, 4):
                sock_uni.sendto(mensaje.encode('utf-8'), destino)
                
                # Espera hasta 1.5 segundos a que el hilo unicast reciba algo
                if respuesta_evento.wait(timeout=1.5):
                    exito = True
                    break
                else:
                    log(f"  [TIMEOUT] Reintentando puja ({intento}/3)...")
            
            if not exito:
                log("  [!] No se pudo contactar al servidor tras 3 intentos.")
            
    except KeyboardInterrupt:
        pass
    finally:
        log("\nCerrando cliente...")
        stop_event.set()
        h1.join(timeout=2)
        h2.join(timeout=2)
        sys.exit(0)