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
CHECK_INTERVAL = 1

# Load Balancer endpoint
LB_URL = "http://127.0.0.1:8080"


# Start/stop worker functions
def start_worker(i):
    port = BASE_PORT + i                                                      # Launcheamos worker por BASE + id
    env = os.environ.copy()
    env["WORKER_ID"] = f"worker{i}"                                           # Creamos el id del worker
    p = subprocess.Popen(                                                     # Lanzamos el worker
        ["uvicorn", "app.main:app", "--port", str(port)],
        env=env
    )
    workers[i] = {"proc": p, "port": port}                                    # Lo ponemos en la lista d workers para poder controlarlo
    print(f"Started worker {i} on port {port}")

    # Esperar a que el worker esté listo
    time.sleep(0.5)

    # Registrar en el load balancer
    try:
        requests.post(f"{LB_URL}/register", json={"port": port})
    except:
        print("Warning: could not register worker in LB")

# Metodo para parar un trabajador por su id
def stop_worker(i):
    if i in workers:
        port = workers[i]["port"]

        workers[i]["proc"].terminate()
        print(f"Stopped worker {i}")

        # Desregistrar del load balancer
        try:
            requests.post(f"{LB_URL}/unregister", json={"port": port})
        except:
            print("Warning: could not unregister worker from LB")

        del workers[i]

# Metodo para escalar a determinado rango, lo que hace es:
# Nos pasan el numero ideal que deberiamos de tener ahora segun la carga, comparamos con el current (segun lonjitud lista d workers)
def scale_to(n):
    current = len(workers)
    if n > current:                                     # Si lo ideal es mayor en numero --> necesitamos más workers
        for i in range(current, n):                     # Tenemos que cubrir esa diferencia y lanzar más workers
            start_worker(i)
    elif n < current:                                   # Si el numero ideal d workers es menor al actual entonces necesitamos menos workers
        for i in list(workers.keys())[n:]:              # Tenemos q cubrir esa diferenzia parando workers
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