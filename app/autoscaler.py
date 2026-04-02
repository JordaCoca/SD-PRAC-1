import subprocess
import time
import os
import requests
from math import ceil

workers = {}
BASE_PORT = 8001

# Variables donde definimos la carga por cada worker
MIN_WORKERS = 1
MAX_WORKERS = 5
TARGET_RPS_PER_WORKER = 50
CHECK_INTERVAL = 2


# Start/stop worker functions
def start_worker(i):
    port = BASE_PORT + i
    env = os.environ.copy()
    env["WORKER_ID"] = f"worker{i}"
    p = subprocess.Popen(
        ["uvicorn", "app.main:app", "--port", str(port)],
        env=env
    )
    workers[i] = {"proc": p, "port": port}
    print(f"Started worker {i} on port {port}")


def stop_worker(i):
    if i in workers:
        workers[i]["proc"].terminate()
        print(f"Stopped worker {i}")
        del workers[i]


def scale_to(n):
    current = len(workers)
    if n > current:
        for i in range(current, n):
            start_worker(i)
    elif n < current:
        for i in list(workers.keys())[n:]:
            stop_worker(i)


# Función para calcular RPS total
def get_total_rps():
    total = 0
    for w in workers.values():
        try:
            r = requests.get(f"http://127.0.0.1:{w['port']}/metrics")
            data = r.json()
            # sumamos todas las requests de ese worker
            worker_rps = sum(data.values())
            total += worker_rps
        except:
            pass
    return total


# Inicializamos con el mínimo
scale_to(MIN_WORKERS)

# Loop de autoescalado
prev_total = 0
prev_time = time.time()

while True:
    time.sleep(CHECK_INTERVAL)
    total_requests = get_total_rps()
    now = time.time()

    rps = (total_requests - prev_total) / (now - prev_time)
    prev_total = total_requests
    prev_time = now

    desired_workers = ceil(rps / TARGET_RPS_PER_WORKER)
    desired_workers = min(max(desired_workers, MIN_WORKERS), MAX_WORKERS)

    print(f"Current RPS: {rps:.2f}, scaling to {desired_workers} workers")
    scale_to(desired_workers)