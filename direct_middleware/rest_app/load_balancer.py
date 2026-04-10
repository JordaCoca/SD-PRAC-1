import redis
from fastapi import FastAPI, Request
import requests
import itertools
import httpx

app = FastAPI()

workers = []
worker_cycle = None
client = httpx.AsyncClient()
MAX_SEATS = 20000

# Conectamos el Load Balancer a Redis para que pueda hacer el reset global
r_db = redis.Redis(host="localhost", port=6379, decode_responses=True)

def update_cycle():
    global worker_cycle
    worker_cycle = itertools.cycle(workers) if workers else None


@app.post("/register")
def register(data: dict):
    port = data["port"]
    url = f"http://127.0.0.1:{port}"

    if url not in workers:
        workers.append(url)
        update_cycle()
        print(f"Registered {url}")

    return {"status": "ok"}


@app.post("/unregister")
def unregister(data: dict):
    port = data["port"]
    url = f"http://127.0.0.1:{port}"

    if url in workers:
        workers.remove(url)
        update_cycle()
        print(f"Unregistered {url}")

    return {"status": "ok"}


@app.post("/buy")
async def proxy_buy(req: Request):
    if not worker_cycle:
        return {"status": "FAIL", "reason": "no workers"}

    data = await req.json()
    worker = next(worker_cycle)

    # Intentar hasta 3 veces con workers distintos si uno falla
    for _ in range(3):
        worker = next(worker_cycle)
        try:
            # Bajamos un poco el timeout para no hacer esperar al cliente, le aumento el margen d reintento
            r = await client.post(f"{worker}/buy", json=data, timeout=5.0)
            return r.json()
        except (httpx.ConnectError, httpx.TimeoutException):
            print(f" Worker {worker} falló. Reintentando con otro...")
            continue
    return {"status": "FAIL", "reason": "all workers failed"}


@app.get("/metrics")
def get_metrics():
    # Obtenemos todas las llaves de métricas
    keys = r_db.keys("metrics:*")
    stats = {"received": 0, "processed": 0, "success": 0, "fail": 0}

    for k in keys:
        try:
            val = r_db.get(k)
            if val is None: continue
            v = int(val)

            # Filtramos por el SUFIJO exacto para evitar errores de coincidencia
            if k.endswith(":success"):
                stats["success"] += v
            elif k.endswith(":fail"):
                stats["fail"] += v
            elif k.endswith(":requests_received"):
                stats["received"] += v
            elif k.endswith(":requests_processed"):
                stats["processed"] += v
        except Exception as e:
            continue

    return stats


@app.post("/reset")
def proxy_reset():
    # 1. Limpiamos Redis desde aquí mismo
    r_db.flushall()

    # 2. Re-inicializamos los asientos
    seats = list(range(1, MAX_SEATS + 1))

    pipe = r_db.pipeline()
    for i in range(0, len(seats), 5000):
        pipe.sadd("available_seats", *seats[i:i + 5000])
    pipe.execute()

    print(f"--- Redis Reset Global completado: {MAX_SEATS} tickets listos ---")
    return {"status": "OK", "message": "Global reset performed by Load Balancer"}