from fastapi import FastAPI
import redis
import pika

app = FastAPI()

# Conexión a Redis para Reset y para leer Métricas
r_db = redis.Redis(host="localhost", port=6379, decode_responses=True)

@app.post("/reset")
def reset_total():
    # 1. Limpiar Redis (Asientos y Estadísticas)
    r_db.flushall()

    # 2. Inicializar 20,000 asientos
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
        channel.queue_purge(queue='ticket_queue')
        connection.close()
        mq_status = "Queue purged."
    except:
        mq_status = "Queue purge failed (maybe it didn't exist)."

    return {"status": "OK", "detail": f"System wiped. {mq_status}"}


@app.get("/metrics")
def get_metrics():
    aggregated = {
        "received": 0,
        "processed": 0,
        "success": 0
    }

    # Buscamos todas las claves que empiecen por metrics:mq-*
    keys = r_db.keys("metrics:mq-*")
    for key in keys:
        # key suele ser "metrics:mq-1:success", etc.
        val = int(r_db.get(key) or 0)
        if "received" in key: aggregated["received"] += val
        if "processed" in key: aggregated["processed"] += val
        if "success" in key: aggregated["success"] += val

    return aggregated