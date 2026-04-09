import os
import sys
import time
import subprocess
import requests
import pika
import json
from datetime import datetime

# --- CONFIGURACIÓN ---
REST_URL = "http://127.0.0.1:8080"
RESULT_DIR = "resultados"
QUEUE_NAME = 'ticket_queue'
MAX_WORKERS = 8
WORKLOAD = 50000

os.makedirs(RESULT_DIR, exist_ok=True)


def reset_system():
    print("   [Reset] Limpiando Redis y RabbitMQ...")
    requests.post(f"{REST_URL}/reset")

    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)
    channel.queue_purge(queue=QUEUE_NAME)
    connection.close()
    time.sleep(2)


def start_workers(num_workers):
    print(f"   [System] Iniciando {num_workers} worker(s)...")
    procesos = []
    base_path = os.path.dirname(os.path.abspath(__file__))
    worker_script = os.path.join(base_path, "mq_app", "mq_worker.py")
    python_exe = sys.executable

    for i in range(num_workers):
        env = os.environ.copy()
        env["WORKER_ID"] = f"mq-bench-{i + 1}"
        p = subprocess.Popen([python_exe, worker_script], env=env)
        procesos.append(p)
    time.sleep(2)
    return procesos


def stop_workers(procesos):
    print("   [System] Deteniendo workers...")
    for p in procesos:
        p.terminate()
        p.wait()


def inject_workload(mode="unnumbered"):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)

    print(f"   [Inject] Enviando {WORKLOAD} peticiones ({mode})...")
    for i in range(WORKLOAD):
        if mode == "unnumbered":
            # Unnumbered: Todos a None
            message = {"client_id": f"c_{i}", "seat_id": None, "request_id": f"req_{i}"}
        else:
            # Numbered: Patrón cíclico 1 a 2000 para asegurar colisiones masivas
            # Cada asiento se pedirá exactamente 25 veces (50000 / 2000 = 25)
            seat_id = (i % 2000) + 1
            message = {"client_id": f"c_{i}", "seat_id": seat_id, "request_id": f"req_{i}"}

        channel.basic_publish(
            exchange='', routing_key=QUEUE_NAME,
            body=json.dumps(message), properties=pika.BasicProperties(delivery_mode=2)
        )
    connection.close()


def wait_and_measure(start_time):
    print("   [Wait] Procesando...")
    while True:
        try:
            resp = requests.get(f"{REST_URL}/metrics").json()
            procesados = resp.get("processed", 0)
            if procesados >= WORKLOAD:
                end_time = time.time()
                return end_time - start_time, resp
        except:
            pass
        time.sleep(0.5)


def run_benchmark():
    fecha_hora = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    file_path = os.path.join(RESULT_DIR, f"stress_test_50k_{fecha_hora}.txt")

    print(f" Iniciando Benchmark 50k (1-8 Workers) -> {file_path}")

    with open(file_path, "w") as f:
        f.write(f"--- BENCHMARK SISTEMA TICKETS (50.000 PETICIONES) ---\n")
        f.write(f"Fecha: {fecha_hora}\n\n")

        # --- FASE 1: UNNUMBERED ---
        f.write("FASE: UNNUMBERED (20.000 SILLAS DISPONIBLES)\n")
        f.write("Workers | Tiempo (s) | Throughput (msg/s) | Success | Fail\n")
        f.write("-" * 70 + "\n")

        for n in range(1, MAX_WORKERS + 1):
            print(f"\n--- UNNUMBERED: {n} WORKER(S) ---")
            reset_system()
            workers = start_workers(n)

            start_time = time.time()
            inject_workload(mode="unnumbered")
            duration, m = wait_and_measure(start_time)

            stop_workers(workers)

            tp = WORKLOAD / duration
            # .get("fail", 0) evita el KeyError si no hay fallos
            res = f"{n:7} | {duration:10.2f} | {tp:18.2f} | {m.get('success', 0):7} | {m.get('fail', 0):4}"
            print(f"  {res}")
            f.write(res + "\n")
            f.flush()

        # --- FASE 2: NUMBERED ---
        f.write("\nFASE: NUMBERED (ASIENTOS 1-2000 REPETIDOS)\n")
        f.write("Workers | Tiempo (s) | Throughput (msg/s) | Success | Fail\n")
        f.write("-" * 70 + "\n")

        for n in range(1, MAX_WORKERS + 1):
            print(f"\n--- NUMBERED: {n} WORKER(S) ---")
            reset_system()
            workers = start_workers(n)

            start_time = time.time()
            inject_workload(mode="numbered")
            duration, m = wait_and_measure(start_time)

            stop_workers(workers)

            tp = WORKLOAD / duration
            res = f"{n:7} | {duration:10.2f} | {tp:18.2f} | {m.get('success', 0):7} | {m.get('fail', 0):4}"
            print(f"  {res}")
            f.write(res + "\n")
            f.flush()

    print(f"\n Test de estrés completado con éxito.")


if __name__ == "__main__":
    run_benchmark()