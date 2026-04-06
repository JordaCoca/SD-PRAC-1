import time
import subprocess
import pika
from math import ceil

# --- CONFIGURACIÓN ---
QUEUE_NAME = 'ticket_queue'
RABBIT_HOST = 'localhost'

MIN_WORKERS = 1
MAX_WORKERS = 10
MESSAGES_PER_WORKER = 50  # Queremos 1 worker por cada 500 mensajes en cola
SCALE_DOWN_COOLDOWN = 5  # Segundos de espera antes de apagar un worker
CHECK_INTERVAL = 2  # Cada cuánto tiempo mirar la cola

workers = {}  # {id: subprocess_handle}
last_scale_down_time = time.time()


def get_queue_depth():
    """Consulta a RabbitMQ cuántos mensajes hay en la cola"""
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=RABBIT_HOST))
        channel = connection.channel()
        # passive=True sirve para solo consultar el estado sin crear la cola
        queue = channel.queue_declare(queue=QUEUE_NAME, durable=True, passive=True)
        count = queue.method.message_count
        connection.close()
        return count
    except Exception as e:
        print(f" Error consultando RabbitMQ: {e}")
        return 0


def start_worker(worker_id):
    if worker_id not in workers:
        print(f" Escalando: Iniciando Worker MQ {worker_id}")
        # Pasamos el ID por variable de entorno para que las métricas en Redis sean separadas
        env = {"WORKER_ID": f"mq-{worker_id}"}
        # IMPORTANTE: Asegúrate de que el nombre del archivo sea el de tu consumidor MQ
        proc = subprocess.Popen(["python", "app/worker_mq.py"], env=env)
        workers[worker_id] = proc


def stop_worker(worker_id):
    if worker_id in workers:
        print(f"Desescalando: Deteniendo Worker MQ {worker_id}")
        workers[worker_id].terminate()  # SIGTERM para un cierre limpio
        del workers[worker_id]


def monitor_and_scale():
    global last_scale_down_time

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