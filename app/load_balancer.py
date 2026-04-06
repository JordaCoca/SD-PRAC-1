import redis
from fastapi import FastAPI, Request
import requests
import itertools
import httpx

app = FastAPI()

workers = []
worker_cycle = None
client = httpx.AsyncClient()
MAX_SEATS = 2000

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
            # Bajamos un poco el timeout para no hacer esperar al cliente
            r = await client.post(f"{worker}/buy", json=data, timeout=1.5)
            return r.json()
        except (httpx.ConnectError, httpx.TimeoutException):
            print(f" Worker {worker} falló. Reintentando con otro...")
            continue
    return {"status": "FAIL", "reason": "all workers failed"}



@app.get("/metrics")
def metrics():
    aggregated = {}

    for w in workers:
        try:
            r = requests.get(f"{w}/metrics", timeout=0.5)
            data = r.json()

            for k, v in data.items():
                aggregated[k] = aggregated.get(k, 0) + v
        except:
            pass

    return aggregated


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