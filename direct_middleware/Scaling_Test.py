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
WORKLOAD = 50000
MAX_WORKERS = 8
CONCURRENCY = 100  # Nivel de clientes simultáneos

os.makedirs(RESULT_DIR, exist_ok=True)

# --- CONFIGURACIÓN DE SESIÓN (evitar WinError 10048) ---
session = requests.Session()
# Reintentos automáticos por si el LB está muy saturado
retries = Retry(total=5, backoff_factor=0.1, status_forcelist=[500, 502, 503, 504])
adapter = HTTPAdapter(pool_connections=CONCURRENCY, pool_maxsize=CONCURRENCY, max_retries=retries)
session.mount('http://', adapter)


def reset_system():
    print("   [Reset] Limpiando sistema...")
    try:
        session.post(f"{LB_URL}/reset", timeout=10)
    except Exception as e:
        print(f"Error en reset: {e}")
    time.sleep(2)


def start_rest_workers(n):
    print(f"   [System] Levantando {n} worker(s) REST...")
    procesos = []
    for i in range(1, n + 1):
        port = 8000 + i
        env = os.environ.copy()
        env["WORKER_ID"] = f"mq-bench-{i}"  # Usamos el mismo ID para métricas

        # Lanzamos uvicorn. IMPORTANTE: El working directory debe ser la raíz del proyecto
        p = subprocess.Popen(
            ["uvicorn", "rest_app.main:app", "--port", str(port), "--log-level", "critical"],
            env=env
        )
        procesos.append(p)

        # Dar tiempo a que arranque y registrarlo en el LB
        time.sleep(0.8)
        try:
            session.post(f"{LB_URL}/register", json={"port": port}, timeout=2)
        except:
            print(f"Error registrando worker en puerto {port}")

    return procesos


def stop_workers(procesos):
    print("   [System] Deteniendo workers...")
    for p in procesos:
        p.terminate()
        p.wait()


def send_request(payload):
    """Lanza la petición usando la sesión reutilizable"""
    try:
        session.post(f"{LB_URL}/buy", json=payload, timeout=10)
    except:
        pass


def run_benchmark_rest():
    fecha_hora = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    file_path = os.path.join(RESULT_DIR, f"benchmark_rest_{fecha_hora}.txt")

    print(f" Iniciando Benchmark REST (1-8 Workers) -> {file_path}")

    with open(file_path, "w") as f:
        f.write(f"--- BENCHMARK SISTEMA REST (50.000 PETICIONES) ---\n")
        f.write(f"Fecha: {fecha_hora}\n\n")

        for mode in ["unnumbered", "numbered"]:
            f.write(f"FASE: {mode.upper()}\n")
            f.write("Workers | Tiempo (s) | Throughput (msg/s) | Success | Fail\n")
            f.write("-" * 75 + "\n")

            for n in range(1, MAX_WORKERS + 1):
                print(f"\n--- {mode.upper()}: {n} WORKER(S) ---")
                reset_system()
                workers = start_rest_workers(n)

                # Preparamos payloads
                payloads = []
                for i in range(WORKLOAD):
                    s_id = None if mode == "unnumbered" else (i % 2000) + 1
                    payloads.append({"client_id": f"c_{i}", "seat_id": s_id, "request_id": f"req_{i}"})

                print(f"   [Inject] Enviando carga...")
                start_time = time.time()

                # Ataque masivo con hilos
                with ThreadPoolExecutor(max_workers=CONCURRENCY) as executor:
                    executor.map(send_request, payloads)

                # Espera a que las métricas confirmen el fin
                print("   [Wait] Esperando métricas finales...")
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

                stop_workers(workers)

                tp = WORKLOAD / duration
                res = f"{n:7} | {duration:10.2f} | {tp:18.2f} | {final_metrics.get('success', 0):7} | {final_metrics.get('fail', 0):4}"
                print(f"  {res}")
                f.write(res + "\n")
                f.flush()

    print(f"\n Test REST completado. Datos guardados en {file_path}")


if __name__ == "__main__":
    run_benchmark_rest()