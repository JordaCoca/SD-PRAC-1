import os
import subprocess
import sys
import time
import pika
from math import ceil

# Configuración
QUEUE_NAME = 'ticket_queue'
RABBIT_HOST = 'localhost'
MIN_WORKERS = 1
MAX_WORKERS = 10
CHECK_INTERVAL = 0.5
SCALE_DOWN_COOLDOWN = 1.5

MESSAGES_PER_WORKER = 500

workers = {}


def get_queue_depth():
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBIT_HOST))
        channel = connection.channel()
        # IMPORTANTE: passive=False (por defecto) para que la cree si no existe
        queue = channel.queue_declare(queue=QUEUE_NAME, durable=True)
        count = queue.method.message_count
        connection.close()
        return count
    except Exception as e:
        print(f" Esperando a RabbitMQ... {e}")
        return 0


def start_worker(worker_id):
    if worker_id not in workers:
        print(f" Escalando: Iniciando Worker MQ {worker_id}")

        # 1. Obtenemos la ruta absoluta de la carpeta donde está este script (mq_app)
        base_path = os.path.dirname(os.path.abspath(__file__))
        worker_script = os.path.join(base_path, "mq_worker.py")

        # 2. Usamos el ejecutable de Python que está corriendo actualmente
        # Esto es mucho mejor que poner ".venv/..." a mano
        python_exe = sys.executable

        # 3. Copiamos el entorno (Vital para el WinError 10106)
        env = os.environ.copy()
        env["WORKER_ID"] = f"mq-{worker_id}"

        # 4. Lanzamos el proceso
        proc = subprocess.Popen(
            [python_exe, worker_script],
            env=env
        )
        workers[worker_id] = proc

def stop_worker(worker_id):
    if worker_id in workers:
        print(f"Desescalando: Deteniendo Worker MQ {worker_id}")
        workers[worker_id].terminate()  # SIGTERM para un cierre limpio
        del workers[worker_id]


def monitor_and_scale():
    global last_scale_down_time
    last_scale_down_time = time.time()

    # Asegurar el mínimo de workers al arrancar
    for i in range(MIN_WORKERS):
        start_worker(i)

    print(f"--- Autoscaler MQ iniciado (Min: {MIN_WORKERS}, Max: {MAX_WORKERS}) ---")

    while True:
        pending_messages = get_queue_depth()
        current_workers = len(workers)

        # Lógica de decisión: ¿Cuántos workers necesito para vaciar esto?
        desired_workers = ceil(pending_messages / MESSAGES_PER_WORKER)
        desired_workers = max(MIN_WORKERS, min(desired_workers, MAX_WORKERS))

        print(f"Cola: {pending_messages} | Activos: {current_workers} | Deseados: {desired_workers}")

        # --- ESCALAR HACIA ARRIBA ---
        if desired_workers > current_workers:
            for i in range(current_workers, desired_workers):
                start_worker(i)

        # --- ESCALAR HACIA ABAJO (con Cooldown) ---
        elif desired_workers < current_workers:
            if time.time() - last_scale_down_time > SCALE_DOWN_COOLDOWN:
                # Quitamos el último de la lista
                id_to_stop = list(workers.keys())[-1]
                stop_worker(id_to_stop)
                last_scale_down_time = time.time()

        time.sleep(CHECK_INTERVAL)


if __name__ == "__main__":
    monitor_and_scale()