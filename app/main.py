from fastapi import FastAPI
from pydantic import BaseModel
import redis
import os
from typing import Optional

app = FastAPI()

r = redis.Redis(host="localhost", port=6379, decode_responses=True)

MAX_SEATS = 20000
WORKER_ID = os.getenv("WORKER_ID", "worker-unknown")


class BuyRequest(BaseModel):
    client_id: str
    seat_id: Optional[int]          # Permitir unnumbered
    request_id: str


@app.post("/buy")
def buy(req: BuyRequest):
    r.incr(f"metrics:{WORKER_ID}:requests")

    # caso unnumbered
    if req.seat_id is None:
        for seat in range(1, MAX_SEATS + 1):
            key = f"seat:{seat}"
            if r.setnx(key, req.client_id):
                return {"status": "SUCCESS"}

        return {"status": "FAIL", "reason": "sold out"}

    # caso numbered
    if req.seat_id < 1 or req.seat_id > MAX_SEATS:
        return {"status": "FAIL", "reason": "invalid seat"}

    key = f"seat:{req.seat_id}"

    if r.setnx(key, req.client_id):
        return {"status": "SUCCESS"}
    else:
        return {"status": "FAIL"}


@app.post("/reset")
def reset():
    r.flushall()
    return {"status": "OK"}


@app.get("/metrics")
def metrics():
    keys = r.keys("metrics:*:requests")
    result = {}
    for k in keys:
        result[k] = int(r.get(k))
    return result