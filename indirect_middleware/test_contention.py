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
MAX_WORKERS_SCALING = 8  # Para los tests de escalabilidad (Fase 1 y 2)
FIXED_WORKERS_CONTENTION = 4  # Workers fijos para el test de contención (Fase 3)
WORKLOAD = 50000

os.makedirs(RESULT_DIR, exist_ok=True)


def reset_system():
    print("   [Reset] Limpiando Redis y RabbitMQ...")
    requests.post(f"{REST_URL}/reset")
    # RabbitMQ Purge
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()
        channel.queue_declare(queue=QUEUE_NAME, durable=True)
        channel.queue_purge(queue=QUEUE_NAME)
        connection.close()
    except:
        pass
    time.sleep(2)


def start_workers(num_workers):
    print(f"   [System] Iniciando {num_workers} worker(s) MQ...")
    procesos = []
    base_path = os.path.dirname(os.path.abspath(__file__))
    # Ajusta esta ruta según tu estructura (donde esté mq_worker.py)
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
    for p in procesos:
        p.terminate()
        p.wait()


def inject_workload(num_seats=None):
    """
    num_seats = None -> Unnumbered
    num_seats = int  -> Numbered con ese rango de asientos
    """
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)

    mode_str = "unnumbered" if num_seats is None else f"numbered ({num_seats} seats)"
    print(f"   [Inject] Enviando {WORKLOAD} peticiones ({mode_str})...")

    for i in range(WORKLOAD):
        if num_seats is None:
            seat_id = None
        else:
            seat_id = (i % num_seats) + 1

        message = {"client_id": f"c_{i}", "seat_id": seat_id, "request_id": f"req_{i}"}
        channel.basic_publish(
            exchange='', routing_key=QUEUE_NAME,
            body=json.dumps(message), properties=pika.BasicProperties(delivery_mode=2)
        )
    connection.close()


def wait_and_measure(start_time):
    while True:
        try:
            resp = requests.get(f"{REST_URL}/metrics").json()
            if resp.get("processed", 0) >= WORKLOAD:
                return time.time() - start_time, resp
        except:
            pass
        time.sleep(0.5)


def run_benchmark():
    fecha_hora = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    file_path = os.path.join(RESULT_DIR, f"full_benchmark_{fecha_hora}.txt")

    with open(file_path, "w") as f:
        f.write(f"--- BENCHMARK INTEGRAL (RABBITMQ) ---\n")
        f.write(f"Fecha: {fecha_hora} | Workload: {WORKLOAD}\n\n")

        # --- FASE 1: ESCALABILIDAD (Variable: Workers, Fijo: Asientos) ---
        f.write("FASE 1: ESCALABILIDAD UNNUMBERED\n")
        f.write("Workers | Tiempo (s) | Throughput (msg/s) | Success | Fail\n" + "-" * 70 + "\n")
        for n in range(1, MAX_WORKERS_SCALING + 1):
            reset_system()
            w_procs = start_workers(n)
            start_t = time.time()
            inject_workload(num_seats=None)
            duration, m = wait_and_measure(start_t)
            stop_workers(w_procs)
            res = f"{n:7} | {duration:10.2f} | {WORKLOAD / duration:18.2f} | {m.get('success', 0):7} | {m.get('fail', 0):4}"
            f.write(res + "\n")
            f.flush()

        # --- FASE 2: CONTENCIÓN (Variable: Asientos, Fijo: Workers) ---
        f.write("\nFASE 2: ANALISIS DE CONTENCION (Workers fijos: {})\n".format(FIXED_WORKERS_CONTENTION))
        f.write("Asientos | Tiempo (s) | Throughput (msg/s) | Success | Fail\n" + "-" * 70 + "\n")

        escenarios_asientos = [20000, 2000, 200, 20]
        for s in escenarios_asientos:
            print(f"\n--- CONTENCION: {s} ASIENTOS CON {FIXED_WORKERS_CONTENTION} WORKERS ---")
            reset_system()
            w_procs = start_workers(FIXED_WORKERS_CONTENTION)
            start_t = time.time()
            inject_workload(num_seats=s)
            duration, m = wait_and_measure(start_t)
            stop_workers(w_procs)
            res = f"{s:8} | {duration:10.2f} | {WORKLOAD / duration:18.2f} | {m.get('success', 0):7} | {m.get('fail', 0):4}"
            f.write(res + "\n")
            f.flush()

    print(f"\nBenchmark completado. Resultados en {file_path}")


if __name__ == "__main__":
    run_benchmark()