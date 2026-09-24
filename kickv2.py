import sys
import time
import random
import datetime
import threading
import asyncio
import websockets
import json
import os
from threading import Thread, Semaphore
import tls_client

CLIENT_TOKEN = "e1393935a959b4020a4491574f6490129f678acdaa92760471263db43487f823"

channel = ""
channel_id = None
stream_id = None
max_threads = 0
threads = []
thread_limit = None
active = 0
stop = False
start_time = None
lock = threading.Lock()
connections = 0
attempts = 0
pings = 0
heartbeats = 0
viewers = 0
last_check = 0

# Configuración de estabilidad (inspirada en Kick-Viewerbot)
STABILITY_MODE = True          # Conexiones de larga duración con reconexión
MAX_RECONNECT_ATTEMPTS = 5     # Intentos máximos antes de abandonar
BASE_RECONNECT_DELAY = 5       # Segundos base para backoff exponencial
HEARTBEAT_MIN = 25             # Mínimo segundos entre heartbeats
HEARTBEAT_MAX = 50             # Máximo segundos entre heartbeats

# Lista de proxies (opcional, formato: "http://user:pass@ip:port" o "socks5://ip:port")
# Si está vacía, se conecta directamente sin proxy
PROXY_LIST = [
    # "http://user:pass@residential-ip:port",
    # "socks5://residential-ip:port",
]

def get_random_proxy():
    """Devuelve un proxy aleatorio de la lista si existe, o None."""
    if PROXY_LIST:
        return random.choice(PROXY_LIST)
    return None

def clean_channel_name(name):
    if "kick.com/" in name:
        parts = name.split("kick.com/")
        channel = parts[1].split("/")[0].split("?")[0]
        return channel.lower()
    return name.lower()

def get_channel_info(name):
    global channel_id, stream_id
    try:
        s = tls_client.Session(client_identifier="chrome_120", random_tls_extension_order=True)
        s.headers.update({
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Referer': 'https://kick.com/',
            'Origin': 'https://kick.com',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
        })
        
        try:
            response = s.get(f'https://kick.com/api/v2/channels/{name}')
            if response.status_code == 200:
                data = response.json()
                channel_id = data.get("id")
                if 'livestream' in data and data['livestream']:
                    stream_id = data['livestream'].get('id')
                return channel_id
        except:
            pass
        
        try:
            response = s.get(f'https://kick.com/api/v1/channels/{name}')
            if response.status_code == 200:
                data = response.json()
                channel_id = data.get("id")
                if 'livestream' in data and data['livestream']:
                    stream_id = data['livestream'].get('id')
                return channel_id
        except:
            pass
        
        try:
            response = s.get(f'https://kick.com/{name}')
            if response.status_code == 200:
                import re
                
                patterns = [
                    r'"id":(\d+).*?"slug":"' + re.escape(name) + r'"',
                    r'"channel_id":(\d+)',
                    r'channelId["\']:\s*(\d+)',
                ]
                for pattern in patterns:
                    match = re.search(pattern, response.text, re.IGNORECASE)
                    if match:
                        channel_id = int(match.group(1))
                        break
                
                stream_patterns = [
                    r'"livestream":\s*\{[^}]*"id":(\d+)',
                    r'livestream.*?"id":(\d+)',
                ]
                for pattern in stream_patterns:
                    match = re.search(pattern, response.text, re.IGNORECASE | re.DOTALL)
                    if match:
                        stream_id = int(match.group(1))
                        break
                
                if channel_id:
                    return channel_id
        except:
            pass
        
        print(f"Failed to get info for: {name}")
        return None
    except Exception as e:
        print(f"Error: {e}")
        return None
    finally:
        if channel_id:
            print(f"Channel ID: {channel_id}")
        if stream_id:
            print(f"Stream ID: {stream_id}")

def get_token():
    try:
        s = tls_client.Session(client_identifier="chrome_120", random_tls_extension_order=True)
        s.headers.update({
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7',
            'Accept-Language': 'en-US,en;q=0.9',
            'Connection': 'keep-alive',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Sec-Fetch-User': '?1',
            'Upgrade-Insecure-Requests': '1',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
        })
        
        try:
            s.get("https://kick.com")
            s.headers["X-CLIENT-TOKEN"] = CLIENT_TOKEN
            response = s.get('https://websockets.kick.com/viewer/v1/token')
            if response.status_code == 200:
                data = response.json()
                token = data.get("data", {}).get("token")
                if token:
                    return token
        except:
            pass
        
        endpoints = [
            'https://websockets.kick.com/viewer/v1/token',
            'https://kick.com/api/websocket/token',
            'https://kick.com/api/v1/websocket/token'
        ]
        for endpoint in endpoints:
            try:
                s.headers["X-CLIENT-TOKEN"] = CLIENT_TOKEN
                response = s.get(endpoint)
                if response.status_code == 200:
                    data = response.json()
                    token = data.get("data", {}).get("token") or data.get("token")
                    if token:
                        return token
            except:
                continue
        return None
    except:
        return None

def get_viewer_count():
    global viewers, last_check
    if not stream_id:
        return 0
    
    try:
        s = tls_client.Session(client_identifier="chrome_120", random_tls_extension_order=True)
        s.headers.update({
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Accept-Encoding': 'gzip, deflate, br',
            'Referer': 'https://kick.com/',
            'Origin': 'https://kick.com',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'sec-ch-ua': '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
        })
        
        url = f"https://kick.com/current-viewers?ids[]={stream_id}"
        response = s.get(url)
        
        if response.status_code == 200:
            data = response.json()
            if isinstance(data, list) and len(data) > 0:
                viewers = data[0].get('viewers', 0)
                last_check = time.time()
                return viewers
        return 0
    except:
        return 0

def show_stats():
    global stop, start_time, connections, attempts, pings, heartbeats, viewers, last_check
    print("\n\n\n")
    os.system('cls' if os.name == 'nt' else 'clear')
    
    while not stop:
        try:
            now = time.time()
            
            if now - last_check >= 5:
                get_viewer_count()
            
            with lock:
                if start_time:
                    elapsed = datetime.datetime.now() - start_time
                    duration = f"{int(elapsed.total_seconds())}s"
                else:
                    duration = "0s"
                
                ws_count = connections
                ws_attempts = attempts
                ping_count = pings
                heartbeat_count = heartbeats
                stream_display = stream_id if stream_id else 'N/A'
                viewer_display = viewers if viewers else 'N/A'
                mode = "STABLE" if STABILITY_MODE else "NORMAL"
            
            print("\033[3A", end="")
            print(f"\033[2K\r[x] Mode: \033[33m{mode}\033[0m | Connections: \033[32m{ws_count}\033[0m | Attempts: \033[32m{ws_attempts}\033[0m")
            print(f"\033[2K\r[x] Pings: \033[32m{ping_count}\033[0m | Heartbeats: \033[32m{heartbeat_count}\033[0m | Duration: \033[32m{duration}\033[0m | Stream ID: \033[32m{stream_display}\033[0m")
            print(f"\033[2K\r[x] Viewers: \033[32m{viewer_display}\033[0m | Updated: \033[32m{time.strftime('%H:%M:%S', time.localtime(last_check))}\033[0m")
            sys.stdout.flush()
            time.sleep(1)
        except:
            time.sleep(1)

def connect():
    send_connection()

def send_connection():
    global active, attempts, channel_id, thread_limit
    active += 1
    with lock:
        attempts += 1
    
    try:
        token = get_token()
        if not token:
            return
        
        if not channel_id:
            channel_id = get_channel_info(channel)
            if not channel_id:
                return
        
        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(websocket_handler(token))
        except:
            pass
        finally:
            try:
                loop.close()
            except:
                pass
    except:
        pass
    finally:
        active -= 1
        thread_limit.release()

async def websocket_handler(token):
    """
    Manejador de WebSocket con lógica de estabilidad mejorada.
    Implementa: conexiones de larga duración, heartbeats aleatorios,
    y reconexión con backoff exponencial (inspirado en Kick-Viewerbot).
    """
    global connections, stop, channel_id, heartbeats, pings
    connected = False
    reconnect_attempts = 0
    
    while not stop and reconnect_attempts < MAX_RECONNECT_ATTEMPTS:
        try:
            # Seleccionar proxy aleatorio si hay disponibles
            proxy = get_random_proxy()
            
            url = f"wss://websockets.kick.com/viewer/v1/connect?token={token}"
            
            # Configurar conexión con o sin proxy
            if proxy:
                ws = await websockets.connect(url, proxy=proxy)
            else:
                ws = await websockets.connect(url)
            
            async with ws:
                with lock:
                    connections += 1
                connected = True
                reconnect_attempts = 0  # Resetear contador al conectar exitosamente
                
                # Handshake inicial
                handshake = {
                    "type": "channel_handshake",
                    "data": {"message": {"channelId": channel_id}}
                }
                await ws.send(json.dumps(handshake))
                with lock:
                    heartbeats += 1
                
                # BUCLE PRINCIPAL: Mantener conexión viva indefinidamente
                while not stop:
                    try:
                        # Heartbeat con intervalo aleatorio (anti-detección)
                        ping = {"type": "ping"}
                        await ws.send(json.dumps(ping))
                        with lock:
                            pings += 1
                        
                        # Intervalo aleatorio entre 25-50 segundos
                        sleep_time = random.randint(HEARTBEAT_MIN, HEARTBEAT_MAX)
                        await asyncio.sleep(sleep_time)
                        
                    except websockets.exceptions.ConnectionClosed:
                        # Conexión cerrada, intentar reconectar
                        break
                    except Exception:
                        break
                        
        except Exception as e:
            # Error al conectar, aplicar backoff exponencial
            pass
        finally:
            if connected:
                with lock:
                    if connections > 0:
                        connections -= 1
                connected = False
        
        # Si estamos en modo estabilidad y no hemos alcanzado el límite, reintentar
        if STABILITY_MODE and not stop:
            reconnect_attempts += 1
            # Backoff exponencial: 5s, 10s, 20s, 40s, 80s
            delay = BASE_RECONNECT_DELAY * (2 ** (reconnect_attempts - 1))
            delay = min(delay, 120)  # Máximo 2 minutos
            await asyncio.sleep(delay)
        else:
            break

def run(thread_count, channel_name):
    global max_threads, channel, start_time, threads, thread_limit, channel_id
    max_threads = int(thread_count)
    channel = clean_channel_name(channel_name)
    thread_limit = Semaphore(max_threads)
    start_time = datetime.datetime.now()
    channel_id = get_channel_info(channel)
    threads = []
    
    if not channel_id:
        print("Error: No se pudo obtener el channel_id. Verifica el nombre del canal.")
        return
    
    stats_thread = Thread(target=show_stats, daemon=True)
    stats_thread.start()
    
    # Lanzar hilos de forma controlada
    for i in range(max_threads):
        thread_limit.acquire()
        t = Thread(target=connect, daemon=True)
        threads.append(t)
        t.start()
        time.sleep(0.5)
    
    print(f"\n[✓] {max_threads} viewers lanzados en modo {'ESTABLE' if STABILITY_MODE else 'NORMAL'}.")
    print(f"[*] Proxies configurados: {len(PROXY_LIST)}")
    print("[*] Presiona Ctrl+C para detener.\n")
    
    try:
        for t in threads:
            t.join()
    except KeyboardInterrupt:
        global stop
        stop = True
        print("\nDeteniendo...")

if __name__ == "__main__":
    try:
        os.system('cls' if os.name == 'nt' else 'clear')
        channel_input = input("Enter channel name or URL: ").strip()
        if not channel_input:
            print("Channel name needed.")
            sys.exit(1)
        
        while True:
            try:
                thread_input = int(input("Enter number of viewers: ").strip())
                if thread_input > 0:
                    break
                else:
                    print("Must be greater than 0")
            except ValueError:
                print("Enter a valid number")
        
        run(thread_input, channel_input)
    except KeyboardInterrupt:
        stop = True
        print("Stopping...")
        sys.exit(0)