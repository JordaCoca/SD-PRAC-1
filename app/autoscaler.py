import subprocess
import time
import os
import requests
from math import ceil

workers = {}
BASE_PORT = 8001

# Variables de autoescalado
MIN_WORKERS = 1
MAX_WORKERS = 40
TARGET_RPS_PER_WORKER = 25
CHECK_INTERVAL = 0.5

# Endpoint del Load Balancer
LB_URL = "http://127.0.0.1:8080"


# --- Funciones para arrancar/parar workers ---
def start_worker(i):
    port = BASE_PORT + i
    env = os.environ.copy()
    worker_id = f"worker{i}"
    env["WORKER_ID"] = worker_id

    log_file = open(f"logging/worker_{i}.log", "w")
    p = subprocess.Popen(
        ["uvicorn", "app.main:app", "--port", str(port), "--log-level", "warning"],
        env=env,
        stdout=log_file,
        stderr=log_file
    )

    workers[i] = {"proc": p, "port": port, "id": worker_id}
    print(f"Started worker {i} on port {port}")

    time.sleep(0.5)

    # Registrar en Load Balancer
    try:
        requests.post(f"{LB_URL}/register", json={"port": port})
    except:
        print("Warning: could not register worker in LB")


def stop_worker(i):
    if i in workers:
        port = workers[i]["port"]
        worker_id = workers[i]["id"]

        # Esperar a que termine de procesar requests pendientes (timeout 5s)
        start = time.time()
        while True:
            try:
                r = requests.get(f"http://127.0.0.1:{port}/metrics").json()
                recvd = r.get(f"metrics:{worker_id}:requests_received", 0)
                done = r.get(f"metrics:{worker_id}:requests_processed", 0)
                if done >= recvd or time.time() - start > 5:
                    break
                time.sleep(0.1)
            except:
                break

        workers[i]["proc"].terminate()
        print(f"Stopped worker {i}")

        try:
            requests.post(f"{LB_URL}/unregister", json={"port": port})
        except:
            print("Warning: could not unregister worker from LB")

        del workers[i]


# --- Escalar al número de workers deseado ---
def scale_to(n):
    current = len(workers)
    if n > current:
        for i in range(current, n):
            start_worker(i)
    elif n < current:
        for i in list(workers.keys())[n:]:
            stop_worker(i)


# --- Calcular RPS y pendientes ---
def get_rps_and_pending():
    total_received = 0
    total_processed = 0
    for w in workers.values():
        try:
            r = requests.get(f"http://127.0.0.1:{w['port']}/metrics").json()
            worker_id = w["id"]
            received = r.get(f"metrics:{worker_id}:requests_received", 0)
            processed = r.get(f"metrics:{worker_id}:requests_processed", 0)
            total_received += received
            total_processed += processed
        except:
            pass
    pending = total_received - total_processed
    return total_received, pending


# --- Inicializar con mínimo ---
scale_to(MIN_WORKERS)

# --- Loop de autoescalado ---
prev_total = 0
prev_time = time.time()

while True:
    time.sleep(CHECK_INTERVAL)
    total_received, pending = get_rps_and_pending()
    now = time.time()

    rps = (total_received - prev_total) / (now - prev_time)
    prev_total = total_received
    prev_time = now

    # Número de workers necesarios según RPS objetivo
    desired_workers = ceil(rps / TARGET_RPS_PER_WORKER)

    # Evitar apagar workers que aún tienen requests pendientes
    min_workers_due_to_pending = ceil(pending / TARGET_RPS_PER_WORKER)
    desired_workers = max(desired_workers, min_workers_due_to_pending)

    # Limitar al rango permitido
    desired_workers = min(max(desired_workers, MIN_WORKERS), MAX_WORKERS)

    print(f"Current RPS: {rps:.2f}, Pending: {pending}, scaling to {desired_workers} workers")
    scale_to(desired_workers)