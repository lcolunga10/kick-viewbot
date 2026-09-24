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

# ============ CONFIGURACIÓN DE ESTABILIDAD ============
STABILITY_MODE = True
MAX_RECONNECT_ATTEMPTS = 3        # Reducido para evitar hilos zombie
BASE_RECONNECT_DELAY = 3          # Backoff más suave
HEARTBEAT_MIN = 28                # Ajustado para máxima estabilidad
HEARTBEAT_MAX = 45
MAX_TOTAL_RECONNECTS = 3          # Límite total de reconexiones por hilo
TARGET_MAINTENANCE_INTERVAL = 5   # Cada cuántos segundos revisar conexiones
LAUNCH_DELAY = 0.8                # Más lento = más natural
# ======================================================

PROXY_LIST = [
    # "http://user:pass@residential-ip:port",
    # "socks5://residential-ip:port",
]

def get_random_proxy():
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
    global stop, start_time, connections, attempts, pings, heartbeats, viewers, last_check, active
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
                active_display = active
                mode = "STABLE" if STABILITY_MODE else "NORMAL"
            
            print("\033[4A", end="")
            print(f"\033[2K\r[x] Mode: \033[33m{mode}\033[0m | Connections: \033[32m{ws_count}\033[0m | Active: \033[32m{active_display}\033[0m | Attempts: \033[32m{ws_attempts}\033[0m")
            print(f"\033[2K\r[x] Pings: \033[32m{ping_count}\033[0m | Heartbeats: \033[32m{heartbeat_count}\033[0m | Duration: \033[32m{duration}\033[0m | Stream ID: \033[32m{stream_display}\033[0m")
            print(f"\033[2K\r[x] Viewers: \033[32m{viewer_display}\033[0m | Updated: \033[32m{time.strftime('%H:%M:%S', time.localtime(last_check))}\033[0m")
            print(f"\033[2K\r[x] Target: \033[36m{max_threads}\033[0m | Missing: \033[31m{max(0, max_threads - ws_count)}\033[0m | Status: {'OK' if ws_count >= max_threads * 0.8 else 'RECONNECTING'}")
            sys.stdout.flush()
            time.sleep(1)
        except:
            time.sleep(1)

def connect():
    send_connection()

def send_connection():
    global active, attempts, channel_id, thread_limit
    with lock:
        active += 1
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
        with lock:
            active -= 1
        thread_limit.release()

async def websocket_handler(token):
    """
    Manejador con reconexión silenciosa (sin perder el viewer).
    Intenta reconectar INMEDIATAMENTE antes de aplicar backoff.
    """
    global connections, stop, channel_id, heartbeats, pings
    connected = False
    total_reconnects = 0
    url = f"wss://websockets.kick.com/viewer/v1/connect?token={token}"
    
    while not stop and total_reconnects < MAX_TOTAL_RECONNECTS:
        try:
            proxy = get_random_proxy()
            
            if proxy:
                ws = await websockets.connect(url, proxy=proxy)
            else:
                ws = await websockets.connect(url)
            
            async with ws:
                with lock:
                    connections += 1
                connected = True
                
                handshake = {
                    "type": "channel_handshake",
                    "data": {"message": {"channelId": channel_id}}
                }
                await ws.send(json.dumps(handshake))
                with lock:
                    heartbeats += 1
                
                # BUCLE PRINCIPAL: mantener conexión viva
                while not stop:
                    try:
                        ping = {"type": "ping"}
                        await ws.send(json.dumps(ping))
                        with lock:
                            pings += 1
                        
                        sleep_time = random.randint(HEARTBEAT_MIN, HEARTBEAT_MAX)
                        await asyncio.sleep(sleep_time)
                        
                    except websockets.exceptions.ConnectionClosed:
                        # RECONEXIÓN SILENCIOSA: intentar inmediatamente
                        # sin esperar backoff, para no perder el viewer
                        try:
                            ws = await websockets.connect(url)
                            await ws.send(json.dumps(handshake))
                            with lock:
                                heartbeats += 1
                            continue  # Seguir con el bucle de pings
                        except:
                            break  # Si falla, salir y aplicar backoff
                    except Exception:
                        break
                        
        except Exception:
            pass
        finally:
            if connected:
                with lock:
                    if connections > 0:
                        connections -= 1
                connected = False
        
        # Si la reconexión silenciosa falló, aplicar backoff suave
        if STABILITY_MODE and not stop:
            total_reconnects += 1
            delay = min(BASE_RECONNECT_DELAY * (1.6 ** (total_reconnects - 1)), 30)
            await asyncio.sleep(delay)
        else:
            break

def run(thread_count, channel_name):
    global max_threads, channel, start_time, threads, thread_limit, channel_id, active
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
    
    print(f"\n[✓] Target: {max_threads} viewers en modo ESTABLE")
    print(f"[*] Proxies configurados: {len(PROXY_LIST)}")
    print("[*] Lanzando conexiones gradualmente...\n")
    
    # Lanzamiento inicial gradual
    for i in range(max_threads):
        if stop:
            break
        thread_limit.acquire()
        t = Thread(target=connect, daemon=True)
        threads.append(t)
        t.start()
        time.sleep(LAUNCH_DELAY)
    
    print(f"[✓] Lanzamiento inicial completo. Manteniendo {max_threads} conexiones...\n")
    
    # BUCLE DE MANTENIMIENTO: repone conexiones caídas
    try:
        while not stop:
            with lock:
                current = connections
                active_count = active
            
            # Si hay menos conexiones de las esperadas, lanzar las que faltan
            missing = max_threads - current - active_count
            
            if missing > 0:
                for _ in range(min(missing, 5)):  # Máximo 5 a la vez para no saturar
                    if stop:
                        break
                    if thread_limit.acquire(timeout=1):
                        t = Thread(target=connect, daemon=True)
                        threads.append(t)
                        t.start()
                        time.sleep(LAUNCH_DELAY)
            
            time.sleep(TARGET_MAINTENANCE_INTERVAL)
            
    except KeyboardInterrupt:
        stop = True
        print("\n[*] Deteniendo...")

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