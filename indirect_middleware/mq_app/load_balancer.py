from fastapi import FastAPI
import redis
import pika

app = FastAPI()
r_db = redis.Redis(host="localhost", port=6379, decode_responses=True)


@app.post("/reset")
def reset_total():
    # 1. Limpiar Redis (Borra everything)
    r_db.flushall()

    # 2. Inicializar 20.000 asientos
    MAX_SEATS = 20000
    seats = list(range(1, MAX_SEATS + 1))
    pipe = r_db.pipeline()
    for i in range(0, len(seats), 5000):
        pipe.sadd("available_seats", *seats[i:i + 5000])
    pipe.execute()

    # 3. Limpiar RabbitMQ
    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
        channel = connection.channel()
        channel.queue_declare(queue='ticket_queue', durable=True)
        channel.queue_purge(queue='ticket_queue')
        connection.close()
    except Exception as e:
        print(f"Error purging queue: {e}")

    return {"status": "OK", "detail": "System wiped and ready."}


@app.get("/metrics")
def get_metrics():
    # Buscamos todas las llaves que sigan el patrón de tus workers
    # Tu worker usa: f"metrics:{WORKER_ID}:success", etc.
    keys = r_db.keys("metrics:mq-*")

    total_received = 0
    total_success = 0
    total_fail = 0
    total_processed = 0
    workers_ids = set()

    for key in keys:
        # Extraemos el ID del worker para contarlos
        # key es metrics:mq-bench-1:success -> split(":")[1] es mq-bench-1
        parts = key.split(":")
        if len(parts) >= 2:
            workers_ids.add(parts[1])

        value = int(r_db.get(key) or 0)

        if "requests_received" in key:
            total_received += value
        elif "success" in key:
            total_success += value
        elif "fail" in key:
            total_fail += value
        elif "requests_processed" in key:
            total_processed += value

    # Si por alguna razón processed no está en Redis, lo calculamos
    if total_processed == 0:
        total_processed = total_success + total_fail

    return {
        "received": total_received,
        "processed": total_processed,
        "success": total_success,
        "fail": total_fail,
        "active_workers": len(workers_ids)
    }