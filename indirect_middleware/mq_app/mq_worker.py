import pika
import redis
import json
import os
import time

from pika import PlainCredentials

CONSISTENCY_MODE = os.getenv("CONSISTENCY_MODE", "optimistic")
LOCK_WAIT_TIMEOUT = float(os.getenv("LOCK_WAIT_TIMEOUT", "0.05"))
LOCK_RETRY_SLEEP = float(os.getenv("LOCK_RETRY_SLEEP", "0.001"))
CRITICAL_SECTION_SLEEP = float(os.getenv("CRITICAL_SECTION_SLEEP", "0.003"))


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
            try:
                s_id = int(seat_id)

                if 1 <= s_id <= 20000:
                    if CONSISTENCY_MODE == "pessimistic":
                        success = sell_numbered_pessimistic(s_id, client_id)
                    else:
                        success = sell_numbered_optimistic(s_id, client_id)
                else:
                    success = False

            except (ValueError, TypeError):
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

def acquire_lock(lock_key, owner, timeout=0.05):
    deadline = time.time() + timeout

    while time.time() < deadline:
        acquired = r.set(lock_key, owner, nx=True, ex=5)
        if acquired:
            return True

        time.sleep(LOCK_RETRY_SLEEP)

    return False


def release_lock(lock_key, owner):
    # Versión simple suficiente para la práctica local.
    # Para producción real se usaría Lua para borrar solo si owner coincide.
    if r.get(lock_key) == owner:
        r.delete(lock_key)


def sell_numbered_optimistic(s_id, client_id):
    return r.setnx(f"seat:{s_id}", client_id)


def sell_numbered_pessimistic(s_id, client_id):
    lock_key = f"lock:seat:{s_id}"
    owner = f"{WORKER_ID}:{client_id}:{s_id}"

    got_lock = acquire_lock(lock_key, owner, timeout=LOCK_WAIT_TIMEOUT)

    if not got_lock:
        return False

    try:
        # Simula una sección crítica más larga: comprobación, transacción, escritura, etc.
        # Esto es lo que hace visible la contención.
        time.sleep(CRITICAL_SECTION_SLEEP)

        return r.setnx(f"seat:{s_id}", client_id)

    finally:
        release_lock(lock_key, owner)

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