import subprocess
import time
import os
import requests
from math import ceil

workers = {}
BASE_PORT = 8001

MIN_WORKERS = 1
MAX_WORKERS = 40

TARGET_RPS_PER_WORKER = 90
CHECK_INTERVAL = 0.5

# Control fino
SCALE_UP_STEP = 6
SCALE_DOWN_STEP = 2

SCALE_DOWN_COOLDOWN = 3  # segundos sin presión antes de bajar

ALPHA = 0.3  # smoothing EMA

LB_URL = "http://127.0.0.1:8080"

last_scale_down_time = time.time()
smoothed_rps = 0


# ---------------- WORKERS ----------------

def start_worker(i):
    port = BASE_PORT + i
    env = os.environ.copy()
    env["WORKER_ID"] = f"worker{i}"

    log_file = open(f"logging/worker_{i}.log", "w")

    p = subprocess.Popen(
        ["uvicorn", "app.main:app", "--port", str(port), "--log-level", "warning"],
        env=env,
        stdout=log_file,
        stderr=log_file
    )

    workers[i] = {"proc": p, "port": port}

    print(f"Started worker {i}")
    time.sleep(0.3)

    try:
        requests.post(f"{LB_URL}/register", json={"port": port})
    except:
        pass


def stop_worker(i):
    if i not in workers:
        return

    port = workers[i]["port"]

    workers[i]["proc"].terminate()
    print(f"Stopped worker {i}")

    try:
        requests.post(f"{LB_URL}/unregister", json={"port": port})
    except:
        pass

    del workers[i]


def scale_to(n):
    current = len(workers)

    if n > current:
        for i in range(current, n):
            start_worker(i)

    elif n < current:
        for i in sorted(workers.keys(), reverse=True)[:current - n]:
            stop_worker(i)


# ---------------- METRICS ----------------

def get_metrics():
    try:
        r = requests.get(f"{LB_URL}/metrics", timeout=0.5)
        data = r.json()

        total_received = data.get("metrics:global:requests_received", 0)
        total_processed = data.get("metrics:global:requests_processed", 0)

        pending = max(0, total_received - total_processed)

        return total_received, pending
    except:
        return 0, 0


# ---------------- INIT ----------------

scale_to(MIN_WORKERS)

prev_total = 0
prev_time = time.time()

# ---------------- LOOP ----------------

while True:
    time.sleep(CHECK_INTERVAL)

    total_received, pending = get_metrics()
    now = time.time()

    delta = total_received - prev_total
    dt = now - prev_time

    # Protección
    if delta < 0:
        delta = 0

    rps = delta / dt if dt > 0 else 0

    prev_total = total_received
    prev_time = now

    # -------- SMOOTHING --------
    smoothed_rps = ALPHA * rps + (1 - ALPHA) * smoothed_rps

    # -------- TARGET --------
    desired = ceil(smoothed_rps / TARGET_RPS_PER_WORKER)
    desired = max(desired, ceil(pending / TARGET_RPS_PER_WORKER))

    desired = max(MIN_WORKERS, min(desired, MAX_WORKERS))

    current = len(workers)

    # -------- HYSTERESIS --------
    if desired > current:
        # SCALE UP rápido
        new_target = min(current + SCALE_UP_STEP, desired)
        scale_to(new_target)

    elif desired < current:
        # SCALE DOWN lento con cooldown
        if time.time() - last_scale_down_time > SCALE_DOWN_COOLDOWN:
            new_target = max(current - SCALE_DOWN_STEP, desired)
            scale_to(new_target)
            last_scale_down_time = time.time()

    print(f"RPS(raw): {rps:.1f} | RPS(avg): {smoothed_rps:.1f} | Pending: {pending} | Workers: {current} -> {desired}")