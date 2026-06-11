import pika
import redis
import json
import os
import time

from pika import PlainCredentials

# Configuración
r = redis.Redis(host='localhost', port=6379, decode_responses=True)
WORKER_ID = os.getenv("WORKER_ID", "worker-mq-default")


def callback(ch, method, properties, body):
    try:
        # 1. Cargar datos del mensaje
        data = json.loads(body)
        client_id = data.get('client_id')
        seat_id = data.get('seat_id')

        # 2. Registrar recepción
        r.incr(f"metrics:{WORKER_ID}:requests_received")
        success = False

        # 3. Lógica de negocio con validación
        if seat_id is None:  # --- MODELO UNNUMBERED ---
            # El límite lo da el inventario físico en el SET de Redis
            if r.spop("available_seats"):
                success = True
        else:  # --- MODELO NUMBERED ---
            # Validamos que el asiento esté entre 1 y 20.000 (según enunciado)
            try:
                s_id = int(seat_id)
                if 1 <= s_id <= 20000:
                    # SETNX intenta crear la llave; si ya existe, devuelve False
                    if r.setnx(f"seat:{s_id}", client_id):
                        success = True
                else:
                    # Asiento fuera de rango (ej: 25.000) -> Fallo
                    success = False
            except (ValueError, TypeError):
                # Si el seat_id no es un número válido -> Fallo
                success = False

        # 4. Registro de métricas finales (Éxito o Fallo)
        if success:
            r.incr(f"metrics:{WORKER_ID}:success")
        else:
            r.incr(f"metrics:{WORKER_ID}:fail")

        # Esta métrica es la que usa el benchmark para saber cuándo terminar
        r.incr(f"metrics:{WORKER_ID}:requests_processed")

    except Exception as e:
        print(f" [!] Error crítico en worker {WORKER_ID}: {e}")
    finally:
        # 5. SIEMPRE enviar el ACK para que el mensaje no se quede en "Unacked"
        ch.basic_ack(delivery_tag=method.delivery_tag)


def start_worker():
    credentials = PlainCredentials('admin', 'superpassword')
    parameters = pika.ConnectionParameters('localhost', credentials=credentials)
    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    channel.queue_declare(queue='ticket_queue', durable=True)

    # Importante: No dar más de 1 mensaje a la vez a este worker (Fair Dispatch)
    channel.basic_qos(prefetch_count=1)

    channel.basic_consume(queue='ticket_queue', on_message_callback=callback)

    print(f" [*] {WORKER_ID} esperando tickets. Para salir presiona CTRL+C")
    channel.start_consuming()


if __name__ == "__main__":
    start_worker()