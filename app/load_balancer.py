from fastapi import FastAPI, Request
import requests
import itertools

app = FastAPI()

workers = []
worker_cycle = None


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

    try:
        r = requests.post(f"{worker}/buy", json=data, timeout=2)
        return r.json()
    except:
        return {"status": "FAIL", "reason": "worker error"}


@app.post("/reset")
def proxy_reset():
    for w in workers:
        try:
            requests.post(f"{w}/reset", timeout=2)
        except:
            pass
    return {"status": "OK"}


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