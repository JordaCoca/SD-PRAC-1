import subprocess
import time
import os

workers = {}
BASE_PORT = 8001


def start_worker(i):
    port = BASE_PORT + i

    env = os.environ.copy()
    env["WORKER_ID"] = f"worker{i}"

    p = subprocess.Popen(
        ["uvicorn", "app.main:app", "--port", str(port)],
        env=env
    )

    workers[i] = p
    print(f"Started worker {i} on port {port}")


def stop_worker(i):
    if i in workers:
        workers[i].terminate()
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


# Escenario dinámico
SCENARIO = [
    (0, 1),
    (10, 2),
    (20, 3),
    (30, 1),
]

start_time = time.time()

for t, num in SCENARIO:
    while time.time() - start_time < t:
        time.sleep(0.5)

    print(f"\nScaling to {num} workers at t={t}")
    scale_to(num)

# mantener vivos
while True:
    time.sleep(1)