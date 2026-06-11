import pika
import redis
import json
import os
import time

from pika import PlainCredentials


# ============================================================
# CONFIGURACIÓN
# ============================================================

r = redis.Redis(host="localhost", port=6379, decode_responses=True)

WORKER_ID = os.getenv("WORKER_ID", "worker-hotspot-default")

LOCK_WAIT_TIMEOUT = float(os.getenv("LOCK_WAIT_TIMEOUT", "0.1"))
LOCK_RETRY_SLEEP = float(os.getenv("LOCK_RETRY_SLEEP", "0.001"))
CRITICAL_SECTION_SLEEP = float(os.getenv("CRITICAL_SECTION_SLEEP", "0"))

RABBIT_HOST = os.getenv("RABBIT_HOST", "localhost")
RABBIT_USER = os.getenv("RABBIT_USER", "admin")
RABBIT_PASS = os.getenv("RABBIT_PASS", "superpassword")
QUEUE_NAME = os.getenv("QUEUE_NAME", "ticket_queue")


# ============================================================
# LOCK PESIMISTA
# ============================================================

def acquire_lock(lock_key, owner, timeout):
    deadline = time.time() + timeout

    while time.time() < deadline:
        acquired = r.set(lock_key, owner, nx=True, ex=5)

        if acquired:
            return True

        time.sleep(LOCK_RETRY_SLEEP)

    return False


def release_lock(lock_key, owner):
    # Suficiente para práctica controlada.
    # En producción se haría con Lua para comprobar y borrar atómicamente.
    if r.get(lock_key) == owner:
        r.delete(lock_key)


def sell_numbered_with_lock(s_id, client_id):
    lock_key = f"lock:seat:{s_id}"
    owner = f"{WORKER_ID}:{client_id}:{s_id}"

    got_lock = acquire_lock(lock_key, owner, LOCK_WAIT_TIMEOUT)

    if not got_lock:
        return False

    try:
        # Simula transacción / comprobación / escritura más larga.
        # Esto hace visible el lock contention bajo hotspot.
        time.sleep(CRITICAL_SECTION_SLEEP)

        return r.setnx(f"seat:{s_id}", client_id)

    finally:
        release_lock(lock_key, owner)


# ============================================================
# CALLBACK
# ============================================================

def callback(ch, method, properties, body):
    try:
        data = json.loads(body)

        client_id = data.get("client_id")
        seat_id = data.get("seat_id")

        r.incr(f"metrics:{WORKER_ID}:requests_received")
        success = False

        if seat_id is None:
            # Para este worker experimental mantenemos también unnumbered,
            # aunque lo normal es usarlo para numbered/hotspot.
            if r.spop("available_seats"):
                success = True

        else:
            try:
                s_id = int(seat_id)

                if 1 <= s_id <= 20000:
                    success = sell_numbered_with_lock(s_id, client_id)
                else:
                    success = False

            except (ValueError, TypeError):
                success = False

        pipe = r.pipeline()

        if success:
            pipe.incr(f"metrics:{WORKER_ID}:success")
        else:
            pipe.incr(f"metrics:{WORKER_ID}:fail")

        pipe.incr(f"metrics:{WORKER_ID}:requests_processed")
        pipe.execute()

    except Exception as e:
        print(f" [!] Error crítico en worker hotspot {WORKER_ID}: {e}")

    finally:
        ch.basic_ack(delivery_tag=method.delivery_tag)


# ============================================================
# START
# ============================================================

def start_worker():
    credentials = PlainCredentials(RABBIT_USER, RABBIT_PASS)

    parameters = pika.ConnectionParameters(
        host=RABBIT_HOST,
        credentials=credentials
    )

    connection = pika.BlockingConnection(parameters)
    channel = connection.channel()

    channel.queue_declare(queue=QUEUE_NAME, durable=True)

    channel.basic_qos(prefetch_count=1)

    channel.basic_consume(
        queue=QUEUE_NAME,
        on_message_callback=callback
    )

    print(
        f" [*] {WORKER_ID} HOTSPOT esperando tickets "
        f"(critical_sleep={CRITICAL_SECTION_SLEEP}, lock_timeout={LOCK_WAIT_TIMEOUT})"
    )

    channel.start_consuming()


if __name__ == "__main__":
    start_worker()