import os
import time
import requests
import pika
import json
from datetime import datetime
from pika import PlainCredentials

# --- CONFIGURACIÓN DE RED ---
TOWER_IP = "ip"  # <--- IP
LB_URL = f"http://{TOWER_IP}:8080"
RESULT_DIR = "resultados"
QUEUE_NAME = 'ticket_queue'
WORKLOAD = 50000
MAX_WORKERS = 8

# --- CREDENCIALES RABBITMQ ---
# Usamos las que definiste en tu comando de Docker
RABBIT_USER = 'admin'
RABBIT_PASS = 'superpassword'

os.makedirs(RESULT_DIR, exist_ok=True)


def reset_system():
    """Llama al reset de la torre (limpia Redis y purga RabbitMQ)"""
    print(f"   [Reset] Pidiendo a la torre {TOWER_IP} que limpie el sistema...")
    try:
        requests.post(f"{LB_URL}/reset", timeout=15)
    except Exception as e:
        print(f"   [!] Error en reset: {e}")
    time.sleep(2)


def scale_remote_workers(n):
    """Usa el endpoint /scale del Load Balancer para gestionar workers en la torre"""
    print(f"   [Remote] Escalando a {n} worker(s) en la torre...")
    try:
        resp = requests.post(f"{LB_URL}/scale?num_workers={n}", timeout=15)
        if resp.status_code == 200:
            print(f"   [OK] Torre confirma: {resp.json().get('total_workers')} workers activos.")
    except Exception as e:
        print(f"   [!] Error al escalar remotamente: {e}")
    time.sleep(2)  # Tiempo de cortesía para que los workers conecten a la cola


def inject_workload(mode="unnumbered"):
    """Conecta al RabbitMQ de la torre e inyecta la carga"""
    # Configuración de credenciales para saltar el bloqueo de 'guest'
    credentials = PlainCredentials(RABBIT_USER, RABBIT_PASS)
    parameters = pika.ConnectionParameters(host=TOWER_IP, credentials=credentials)

    try:
        connection = pika.BlockingConnection(parameters)
        channel = connection.channel()
        channel.queue_declare(queue=QUEUE_NAME, durable=True)

        print(f"   [Inject] Enviando {WORKLOAD} peticiones ({mode}) a la torre...")
        for i in range(WORKLOAD):
            if mode == "unnumbered":
                message = {"client_id": f"c_{i}", "seat_id": None, "request_id": f"req_{i}"}
            else:
                seat_id = (i % 2000) + 1
                message = {"client_id": f"c_{i}", "seat_id": seat_id, "request_id": f"req_{i}"}

            channel.basic_publish(
                exchange='',
                routing_key=QUEUE_NAME,
                body=json.dumps(message),
                properties=pika.BasicProperties(delivery_mode=2)
            )
        connection.close()
    except Exception as e:
        print(f"   [!] Error inyectando carga: {e}")


def wait_and_measure(start_time):
    """Consulta las métricas en la torre hasta que se procese everything"""
    print("   [Wait] Procesando en la torre...")
    while True:
        try:
            resp = requests.get(f"{LB_URL}/metrics", timeout=5).json()
            procesados = resp.get("processed", 0)
            if procesados >= WORKLOAD:
                end_time = time.time()
                return end_time - start_time, resp
        except Exception:
            pass
        time.sleep(1)


def run_benchmark():
    fecha_hora = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    file_path = os.path.join(RESULT_DIR, f"dist_stress_mq_50k_PORTATIL.txt")

    print(f" Iniciando Benchmark Distribuido RabbitMQ (Portátil -> Torre) -> {file_path}")

    with open(file_path, "w") as f:
        f.write(f"--- BENCHMARK DISTRIBUIDO MQ (50.000 PETS) ---\n")
        f.write(f"Torre IP: {TOWER_IP}\n\n")

        # --- FASE 1: UNNUMBERED ---
        f.write("FASE: UNNUMBERED\n")
        f.write("Workers | Tiempo (s) | Throughput (msg/s) | Success | Fail\n")
        f.write("-" * 75 + "\n")

        for n in range(1, MAX_WORKERS + 1):
            print(f"\n--- UNNUMBERED: {n} WORKER(S) ---")
            reset_system()
            scale_remote_workers(n)

            start_time = time.time()
            inject_workload(mode="unnumbered")
            duration, m = wait_and_measure(start_time)

            tp = WORKLOAD / duration
            res = f"{n:7} | {duration:10.2f} | {tp:18.2f} | {m.get('success', 0):7} | {m.get('fail', 0):4}"
            print(f"  {res}")
            f.write(res + "\n")
            f.flush()

        # --- FASE 2: NUMBERED ---
        f.write("\nFASE: NUMBERED (COLISIONES)\n")
        f.write("-" * 75 + "\n")

        for n in range(1, MAX_WORKERS + 1):
            print(f"\n--- NUMBERED: {n} WORKER(S) ---")
            reset_system()
            scale_remote_workers(n)

            start_time = time.time()
            inject_workload(mode="numbered")
            duration, m = wait_and_measure(start_time)

            tp = WORKLOAD / duration
            res = f"{n:7} | {duration:10.2f} | {tp:18.2f} | {m.get('success', 0):7} | {m.get('fail', 0):4}"
            print(f"  {res}")
            f.write(res + "\n")
            f.flush()

        # Limpieza final
        scale_remote_workers(0)

    print(f"\n Test completado. Resultados en {file_path}")


if __name__ == "__main__":
    run_benchmark()