from fastapi import FastAPI
from pydantic import BaseModel
import redis
import os
import time
from typing import Optional

app = FastAPI()

r = redis.Redis(host="localhost", port=6379, decode_responses=True)

MAX_SEATS = 2000
WORKER_ID = os.getenv("WORKER_ID", "worker-unknown")


class BuyRequest(BaseModel):
    client_id: str
    seat_id: Optional[int]
    request_id: str


@app.post("/buy")
def buy(req: BuyRequest):
    # 🔥 métrica correcta
    r.incr(f"metrics:{WORKER_ID}:requests_received")

    # 🔥 pequeña latencia para generar backlog (CLAVE para escalar)
    time.sleep(0.005)

    # UNNUMBERED
    if req.seat_id is None:
        seat = r.spop("available_seats")

        if seat is None:
            r.incr(f"metrics:{WORKER_ID}:requests_processed")
            return {"status": "FAIL", "reason": "sold out"}

        r.set(f"seat:{seat}", req.client_id)

        r.incr(f"metrics:{WORKER_ID}:requests_processed")
        return {"status": "SUCCESS", "seat_id": int(seat)}

    # NUMBERED
    if req.seat_id < 1 or req.seat_id > MAX_SEATS:
        r.incr(f"metrics:{WORKER_ID}:requests_processed")
        return {"status": "FAIL", "reason": "invalid seat"}

    key = f"seat:{req.seat_id}"

    if r.setnx(key, req.client_id):
        r.srem("available_seats", req.seat_id)
        r.incr(f"metrics:{WORKER_ID}:requests_processed")
        return {"status": "SUCCESS", "seat_id": req.seat_id}
    else:
        r.incr(f"metrics:{WORKER_ID}:requests_processed")
        return {"status": "FAIL", "reason": "already taken"}


@app.post("/reset")
def reset():
    r.flushall()
    seats = list(range(1, MAX_SEATS + 1))
    r.sadd("available_seats", *seats)
    return {"status": "OK"}


@app.get("/metrics")
def metrics():
    result = {}

    for k in r.keys("metrics:*"):
        result[k] = int(r.get(k))

    return result