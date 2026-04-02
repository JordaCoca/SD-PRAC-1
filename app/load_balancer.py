from fastapi import FastAPI, Request
import requests
import itertools

app = FastAPI()

# Lista dinámica de workers
workers = []

# Iterador round-robin
worker_cycle = None


# --- ACTUALIZAR CICLO ---
def update_cycle():
    global worker_cycle
    if workers:
        worker_cycle = itertools.cycle(workers)
    else:
        worker_cycle = None


# --- REGISTER WORKER ---
@app.post("/register")
def register(data: dict):
    port = data["port"]
    url = f"http://127.0.0.1:{port}"

    if url not in workers:
        workers.append(url)
        update_cycle()
        print(f"Registered worker: {url}")

    return {"status": "ok"}


# --- UNREGISTER WORKER ---
@app.post("/unregister")
def unregister(data: dict):
    port = data["port"]
    url = f"http://127.0.0.1:{port}"

    if url in workers:
        workers.remove(url)
        update_cycle()
        print(f"Unregistered worker: {url}")

    return {"status": "ok"}


# --- PROXY BUY ---
@app.post("/buy")
async def proxy_buy(req: Request):
    if not worker_cycle:
        return {"status": "FAIL", "reason": "no workers available"}

    data = await req.json()
    worker = next(worker_cycle)

    try:
        r = requests.post(f"{worker}/buy", json=data, timeout=2)
        return r.json()
    except:
        return {"status": "FAIL", "reason": "worker error"}


# --- PROXY RESET ---
@app.post("/reset")
def proxy_reset():
    results = []

    for w in workers:
        try:
            r = requests.post(f"{w}/reset", timeout=2)
            results.append(r.json())
        except:
            pass

    return {"status": "OK", "workers": len(workers)}


# --- METRICS (AGREGADAS) ---
@app.get("/metrics")
def metrics():
    aggregated = {}

    for w in workers:
        try:
            r = requests.get(f"{w}/metrics", timeout=2)
            data = r.json()

            for k, v in data.items():
                aggregated[k] = aggregated.get(k, 0) + v

        except:
            pass

    return aggregated