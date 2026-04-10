import os
import time
import subprocess
import requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- CONFIGURACIÓN ---
LB_URL = "http://127.0.0.1:8080"
RESULT_DIR = "resultados"
WORKLOAD = 10000     # Mantenemos 10k para no ahogar a Windows
WORKERS_TO_USE = 4   # Fijamos los workers a 4 (Número óptimo en pruebas)
CONCURRENCY = 20

os.makedirs(RESULT_DIR, exist_ok=True)

# Configuración de Sesión
session = requests.Session()
retries = Retry(total=5, backoff_factor=0.2, status_forcelist=[500, 502, 503, 504])
adapter = HTTPAdapter(pool_connections=CONCURRENCY, pool_maxsize=CONCURRENCY, max_retries=retries)
session.mount('http://', adapter)

def reset_system():
    print("   [Reset] Limpiando sistema...")
    try:
        session.post(f"{LB_URL}/reset", timeout=10)
    except:
        pass
    time.sleep(3)

def start_rest_workers(n):
    print(f"   [System] Levantando {n} worker(s) REST...")
    procesos = []
    for i in range(1, n + 1):
        port = 8000 + i
        env = os.environ.copy()
        env["WORKER_ID"] = f"rest-worker-{i}"
        p = subprocess.Popen(
            ["uvicorn", "rest_app.main:app", "--port", str(port), "--log-level", "critical"],
            env=env
        )
        procesos.append(p)
        time.sleep(1.5)
        try:
            session.post(f"{LB_URL}/register", json={"port": port}, timeout=2)
        except:
            pass
    return procesos

def stop_workers(procesos):
    print("   [System] Deteniendo workers...")
    for p in procesos:
        p.terminate()
    time.sleep(1)
    for p in procesos:
        if p.poll() is None: p.kill()

def send_request(payload):
    try:
        session.post(f"{LB_URL}/buy", json=payload, timeout=10)
    except:
        pass

def run_contention_test():
    fecha_hora = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    file_path = os.path.join(RESULT_DIR, f"rest_contention_{fecha_hora}.txt")

    # Escenarios: (Nombre, Modulo)
    # Modulo define cuántos asientos hay disponibles para la pelea
    escenarios = [
        ("BAJA_CONTENCION (2000 asientos)", 2000),
        ("ALTA_CONTENCION (10 asientos)", 10),
        ("EXTREMA_CONTENCION (1 asiento)", 1)
    ]

    print(f" Iniciando Test de Contención (NUMBERED) -> {file_path}")

    # Arrancamos los workers una sola vez para todos los tests
    workers = start_rest_workers(WORKERS_TO_USE)

    with open(file_path, "w") as f:
        f.write(f"--- BENCHMARK CONTENCIÓN REST (NUMBERED) ---\n")
        f.write(f"Fecha: {fecha_hora}\n")
        f.write(f"Workers fijos: {WORKERS_TO_USE} | Workload: {WORKLOAD} | Concurrency: {CONCURRENCY}\n\n")
        f.write("Escenario | Modulo | Tiempo (s) | Throughput (msg/s) | Success | Fail\n")
        f.write("-" * 80 + "\n")

        for nombre, modulo in escenarios:
            print(f"\n--- TEST: {nombre} ---")
            print("   [Wait] Cooldown de 8s para liberar sockets TCP...")
            time.sleep(8)
            reset_system()

            payloads = []
            for i in range(WORKLOAD):
                s_id = (i % modulo) + 1  # Aquí forzamos la contención
                payloads.append({"client_id": f"c_{i}", "seat_id": s_id, "request_id": f"req_{i}"})

            print(f"   [Inject] Enviando carga...")
            start_time = time.time()

            with ThreadPoolExecutor(max_workers=CONCURRENCY) as executor:
                executor.map(send_request, payloads)

            print("   [Wait] Esperando métricas finales...")
            final_metrics = {}
            while True:
                try:
                    resp = session.get(f"{LB_URL}/metrics", timeout=2).json()
                    # Si el LB procesó la carga (ya sea success o fail application-level)
                    if resp.get("processed", 0) >= WORKLOAD * 0.98: # 98% margen de tolerancia
                        duration = time.time() - start_time
                        final_metrics = resp
                        break
                except:
                    pass
                time.sleep(1)

            tp = WORKLOAD / duration
            res = f"{nombre:30} | {modulo:6} | {duration:10.2f} | {tp:18.2f} | {final_metrics.get('success', 0):7} | {final_metrics.get('fail', 0):4}"
            print(f"  {res}")
            f.write(res + "\n")
            f.flush()

    stop_workers(workers)
    print(f"\n Test de Contención completado.")

if __name__ == "__main__":
    run_contention_test()