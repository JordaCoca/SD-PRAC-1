import os
import time
import requests
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# --- CONFIGURACIÓN ---
TOWER_IP = "ip"  # <--- IP D LA TORRE
LB_URL = f"http://{TOWER_IP}:8080"
RESULT_DIR = "resultados"
WORKLOAD = 15000
MAX_WORKERS = 8
CONCURRENCY = 25

os.makedirs(RESULT_DIR, exist_ok=True)

# Configuración de Sesión
session = requests.Session()
retries = Retry(total=5, backoff_factor=0.2, status_forcelist=[500, 502, 503, 504])
adapter = HTTPAdapter(pool_connections=CONCURRENCY, pool_maxsize=CONCURRENCY, max_retries=retries)
session.mount('http://', adapter)


def reset_system():
    print("   [Reset] Limpiando Redis y métricas en la Torre...")
    try:
        session.post(f"{LB_URL}/reset", timeout=10)
    except Exception as e:
        print(f"Error en reset: {e}")
    time.sleep(2)


def scale_remote_workers(n):
    """Llamada al mando a distancia para gestionar workers en la Torre"""
    print(f"   [Remote] Escalando a {n} worker(s) en la Torre...")
    try:
        # Llamamos a tu nuevo endpoint /scale
        resp = session.post(f"{LB_URL}/scale?num_workers={n}", timeout=15)
        if resp.status_code == 200:
            print(f"   [OK] Torre confirma: {n} workers activos.")
        else:
            print(f"   [!] Error al escalar: {resp.text}")
    except Exception as e:
        print(f"   [!] Fallo de conexión al escalar: {e}")

    # Damos 4 segundos para que los procesos uvicorn arranquen y se registren
    time.sleep(4)


def send_request(payload):
    try:
        session.post(f"{LB_URL}/buy", json=payload, timeout=10)
    except:
        pass


def run_scalability_test():
    fecha_hora = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    file_path = os.path.join(RESULT_DIR, f"dist_rest_scalability_{fecha_hora}.txt")

    print(f" Iniciando Benchmark Distribuido -> {file_path}")

    with open(file_path, "w") as f:
        f.write(f"--- BENCHMARK DISTRIBUIDO REST (PORTÁTIL -> TORRE) ---\n")
        f.write(f"Fecha: {fecha_hora}\n")
        f.write(f"Workload: {WORKLOAD} | Concurrency: {CONCURRENCY}\n\n")
        f.write("Workers | Tiempo (s) | Throughput (msg/s) | Success | Fail\n")
        f.write("-" * 75 + "\n")

        for n in range(1, MAX_WORKERS + 1):
            print(f"\n--- TEST: {n} WORKER(S) ---")

            # 1. Resetear datos
            reset_system()

            # 2. Escalar remotamente (Mando a distancia)
            scale_remote_workers(n)

            payloads = [{"client_id": f"c_{i}", "seat_id": None, "request_id": f"req_{i}"} for i in range(WORKLOAD)]

            print(f"   [Inject] Enviando carga desde el portátil...")
            start_time = time.time()

            with ThreadPoolExecutor(max_workers=CONCURRENCY) as executor:
                executor.map(send_request, payloads)

            print("   [Wait] Esperando métricas de la Torre...")
            final_metrics = {}
            while True:
                try:
                    resp = session.get(f"{LB_URL}/metrics", timeout=2).json()
                    if resp.get("processed", 0) >= WORKLOAD:
                        duration = time.time() - start_time
                        final_metrics = resp
                        break
                except:
                    pass
                time.sleep(1)

            tp = WORKLOAD / duration
            res = f"{n:7} | {duration:10.2f} | {tp:18.2f} | {final_metrics.get('success', 0):7} | {final_metrics.get('fail', 0):4}"
            print(f"  {res}")
            f.write(res + "\n")
            f.flush()

        # Limpiar al finalizar: dejamos 0 workers
        scale_remote_workers(0)

    print(f"\n Test de Escalabilidad Distribuido completado.")


if __name__ == "__main__":
    run_scalability_test()