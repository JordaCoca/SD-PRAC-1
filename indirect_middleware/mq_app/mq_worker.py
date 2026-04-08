import pika
import redis
import json
import os
import time

# Configuración
r = redis.Redis(host='localhost', port=6379, decode_responses=True)
WORKER_ID = os.getenv("WORKER_ID", "worker-mq-default")


def callback(ch, method, properties, body):
    data = json.loads(body)
    client_id = data['client_id']
    seat_id = data.get('seat_id')  # Puede ser None en Unnumbered

    # --- MÉTRICAS ---
    r.incr(f"metrics:{WORKER_ID}:requests_received")
    time.sleep(0.005)  # Latencia artificial del enunciado

    # --- LÓGICA DE NEGOCIO (Consistencia) ---
    success = False

    if seat_id is None:  # UNNUMBERED
        if r.spop("available_seats"):
            success = True
    else:  # NUMBERED
        if r.setnx(f"seat:{seat_id}", client_id):
            success = True

    # --- REGISTRO DE RESULTADO ---
    if success:
        r.incr(f"metrics:{WORKER_ID}:success")
    else:
        r.incr(f"metrics:{WORKER_ID}:fail")
    r.incr(f"metrics:{WORKER_ID}:requests_processed")

    # --- CONFIRMACIÓN (ACK) ---
    # Le dice a Rabbit: "Ya he terminado, puedes borrar el mensaje de la cola"
    ch.basic_ack(delivery_tag=method.delivery_tag)


def start_worker():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    channel.queue_declare(queue='ticket_queue', durable=True)

    # Importante: No dar más de 1 mensaje a la vez a este worker (Fair Dispatch)
    channel.basic_qos(prefetch_count=1)

    channel.basic_consume(queue='ticket_queue', on_message_callback=callback)

    print(f" [*] {WORKER_ID} esperando tickets. Para salir presiona CTRL+C")
    channel.start_consuming()


if __name__ == "__main__":
    start_worker()