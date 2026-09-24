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
debug = False  # Cambia a True para ver errores

# ============ CONFIGURACIÓN DE ESTABILIDAD ============
STABILITY_MODE = True
HEARTBEAT_MIN = 28
HEARTBEAT_MAX = 45
TARGET_MAINTENANCE_INTERVAL = 5
LAUNCH_DELAY = 0.8
WS_TIMEOUT = 15  # Timeout de conexión WebSocket
# ======================================================

PROXY_LIST = []

def log(msg):
    if debug:
        print(f"[DEBUG] {msg}")

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

def _extract_channel_info(name):
    """Función auxiliar SIN globales, devuelve (channel_id, stream_id)."""
    local_channel_id = None
    local_stream_id = None
    try:
        s = tls_client.Session(client_identifier="chrome_120", random_tls_extension_order=True)
        s.headers.update({
            'Accept': 'application/json, text/plain, */*',
            'Accept-Language': 'en-US,en;q=0.9',
            'Referer': 'https://kick.com/',
            'Origin': 'https://kick.com',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        })

        try:
            r = s.get(f'https://kick.com/api/v2/channels/{name}', timeout_seconds=15)
            if r.status_code == 200:
                data = r.json()
                local_channel_id = data.get("id")
                if data.get('livestream'):
                    local_stream_id = data['livestream'].get('id')
                if local_channel_id:
                    return local_channel_id, local_stream_id
        except Exception as e:
            log(f"v2 falló: {e}")

        try:
            r = s.get(f'https://kick.com/api/v1/channels/{name}', timeout_seconds=15)
            if r.status_code == 200:
                data = r.json()
                local_channel_id = data.get("id")
                if data.get('livestream'):
                    local_stream_id = data['livestream'].get('id')
                if local_channel_id:
                    return local_channel_id, local_stream_id
        except Exception as e:
            log(f"v1 falló: {e}")

        try:
            r = s.get(f'https://kick.com/{name}', timeout_seconds=15)
            if r.status_code == 200:
                import re
                patterns = [
                    r'"id":(\d+).*?"slug":"' + re.escape(name) + r'"',
                    r'"channel_id":(\d+)',
                ]
                for pattern in patterns:
                    m = re.search(pattern, r.text, re.IGNORECASE)
                    if m:
                        local_channel_id = int(m.group(1))
                        break
                stream_patterns = [
                    r'"livestream":\s*\{[^}]*"id":(\d+)',
                ]
                for pattern in stream_patterns:
                    m = re.search(pattern, r.text, re.IGNORECASE | re.DOTALL)
                    if m:
                        local_stream_id = int(m.group(1))
                        break
        except Exception as e:
            log(f"scraping falló: {e}")

    except Exception as e:
        log(f"_extract_channel_info error: {e}")
    return local_channel_id, local_stream_id

def get_channel_info(name):
    """Solo para uso del hilo principal. NO sobreescribe si falla."""
    global channel_id, stream_id
    cid, sid = _extract_channel_info(name)
    if cid:
        channel_id = cid
    if sid:
        stream_id = sid
    return channel_id

def get_token():
    try:
        s = tls_client.Session(client_identifier="chrome_120", random_tls_extension_order=True)
        s.headers.update({
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.9',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        })
        try:
            s.get("https://kick.com", timeout_seconds=15)
        except Exception as e:
            log(f"kick.com falló: {e}")
        s.headers["X-CLIENT-TOKEN"] = CLIENT_TOKEN
        try:
            r = s.get('https://websockets.kick.com/viewer/v1/token', timeout_seconds=15)
            if r.status_code == 200:
                data = r.json()
                token = data.get("data", {}).get("token")
                if token:
                    return token
                log(f"token vacío en respuesta: {data}")
            else:
                log(f"token status {r.status_code}")
        except Exception as e:
            log(f"get_token error: {e}")
        return None
    except Exception as e:
        log(f"get_token outer error: {e}")
        return None

def get_viewer_count():
    global viewers, last_check
    if not stream_id:
        return 0
    try:
        s = tls_client.Session(client_identifier="chrome_120", random_tls_extension_order=True)
        s.headers.update({
            'Accept': 'application/json',
            'Referer': 'https://kick.com/',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        })
        r = s.get(f"https://kick.com/current-viewers?ids[]={stream_id}", timeout_seconds=10)
        if r.status_code == 200:
            data = r.json()
            if isinstance(data, list) and len(data) > 0:
                viewers = data[0].get('viewers', 0)
                last_check = time.time()
                return viewers
    except Exception as e:
        log(f"viewer_count error: {e}")
    return 0

def show_stats():
    global stop, start_time, connections, attempts, pings, heartbeats, viewers, last_check, active
    print("\n\n\n")
    while not stop:
        try:
            now = time.time()
            if now - last_check >= 5:
                get_viewer_count()

            with lock:
                elapsed = datetime.datetime.now() - start_time if start_time else datetime.timedelta(0)
                duration = f"{int(elapsed.total_seconds())}s"
                ws_count = connections
                ws_attempts = attempts
                ping_count = pings
                heartbeat_count = heartbeats
                stream_display = stream_id if stream_id else 'N/A'
                viewer_display = viewers if viewers else 0
                active_display = active

            os.system('cls' if os.name == 'nt' else 'clear')
            print(f"[x] Mode: STABLE | Connections: {ws_count} | Active: {active_display} | Attempts: {ws_attempts}")
            print(f"[x] Pings: {ping_count} | Heartbeats: {heartbeat_count} | Duration: {duration} | Stream ID: {stream_display}")
            print(f"[x] Viewers: {viewer_display} | Updated: {time.strftime('%H:%M:%S', time.localtime(last_check))}")
            print(f"[x] Target: {max_threads} | Missing: {max(0, max_threads - ws_count)} | Status: {'OK' if ws_count >= max_threads * 0.8 else 'RECONNECTING'}")
            sys.stdout.flush()
            time.sleep(1)
        except Exception as e:
            log(f"show_stats error: {e}")
            time.sleep(1)

def send_connection():
    global active, attempts, channel_id, thread_limit
    acquired = False
    try:
        with lock:
            active += 1
            attempts += 1

        token = get_token()
        if not token:
            log("sin token, abortando hilo")
            return

        # Usar channel_id global (ya validado en run), si no, extraer sin tocar globales
        cid = channel_id
        if not cid:
            cid, _ = _extract_channel_info(channel)
            if not cid:
                log("sin channel_id, abortando hilo")
                return

        try:
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(websocket_handler(token, cid))
        except Exception as e:
            log(f"loop error: {e}")
        finally:
            try:
                loop.close()
            except Exception:
                pass
    except Exception as e:
        log(f"send_connection error: {e}")
    finally:
        with lock:
            active -= 1
        # Solo liberar si este hilo había adquirido el semáforo
        try:
            thread_limit.release()
        except Exception:
            pass

async def websocket_handler(token, cid):
    global connections, stop, heartbeats, pings
    connected = False
    url = f"wss://websockets.kick.com/viewer/v1/connect?token={token}"

    try:
        proxy = get_random_proxy()
        if proxy:
            ws = await asyncio.wait_for(
                websockets.connect(url, proxy=proxy), timeout=WS_TIMEOUT
            )
        else:
            ws = await asyncio.wait_for(
                websockets.connect(url), timeout=WS_TIMEOUT
            )

        async with ws:
            with lock:
                connections += 1
            connected = True

            handshake = {
                "type": "channel_handshake",
                "data": {"message": {"channelId": cid}}
            }
            await ws.send(json.dumps(handshake))
            with lock:
                heartbeats += 1

            while not stop:
                try:
                    ping = {"type": "ping"}
                    await ws.send(json.dumps(ping))
                    with lock:
                        pings += 1

                    sleep_time = random.randint(HEARTBEAT_MIN, HEARTBEAT_MAX)
                    await asyncio.sleep(sleep_time)

                except websockets.exceptions.ConnectionClosed:
                    log("conexión cerrada")
                    break
                except Exception as e:
                    log(f"ping error: {e}")
                    break
    except asyncio.TimeoutError:
        log("timeout conectando WS")
    except Exception as e:
        log(f"websocket_handler error: {e}")
    finally:
        if connected:
            with lock:
                if connections > 0:
                    connections -= 1

def run(thread_count, channel_name):
    global max_threads, channel, start_time, threads, thread_limit, channel_id, active
    max_threads = int(thread_count)
    channel = clean_channel_name(channel_name)
    thread_limit = Semaphore(max_threads)
    start_time = datetime.datetime.now()
    threads = []

    print(f"[*] Buscando info del canal: {channel}...")
    cid = get_channel_info(channel)
    if not cid:
        print(f"[!] Error: No se pudo obtener channel_id de '{channel}'. Verifica que el canal exista.")
        return
    print(f"[✓] Channel ID: {cid} | Stream ID: {stream_id if stream_id else 'OFFLINE'}")

    stats_thread = Thread(target=show_stats, daemon=True)
    stats_thread.start()

    print(f"\n[✓] Target: {max_threads} viewers")
    print(f"[*] Proxies: {len(PROXY_LIST)}")
    print("[*] Lanzando conexiones gradualmente...\n")

    # Lanzamiento inicial
    for i in range(max_threads):
        if stop:
            break
        thread_limit.acquire()
        t = Thread(target=send_connection, daemon=True)
        threads.append(t)
        t.start()
        time.sleep(LAUNCH_DELAY)

    print(f"[✓] Lanzamiento inicial completo.\n")

    # Bucle de mantenimiento
    try:
        while not stop:
            with lock:
                current = connections
                active_count = active

            missing = max_threads - current - active_count

            if missing > 0:
                for _ in range(min(missing, 5)):
                    if stop:
                        break
                    if thread_limit.acquire(timeout=1):
                        t = Thread(target=send_connection, daemon=True)
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