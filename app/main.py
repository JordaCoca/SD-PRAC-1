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
    # UNNUMBERED
    if req.seat_id is None:
        seat = r.spop("available_seats")  # atomic

        if seat is None:
            r.incr(f"metrics:{WORKER_ID}:requests_processed")
            return {"status": "FAIL", "reason": "sold out"}

        key = f"seat:{seat}"
        r.set(key, req.client_id)

        r.incr(f"metrics:{WORKER_ID}:requests_processed")
        return {"status": "SUCCESS", "seat_id": int(seat)}
    # NUMBERED
    if req.seat_id < 1 or req.seat_id > MAX_SEATS:
        r.incr(f"metrics:{WORKER_ID}:requests_processed")
        return {"status": "FAIL", "reason": "invalid seat"}

    key = f"seat:{req.seat_id}"

    # intentar reservar ese asiento concreto
    if r.setnx(key, req.client_id):
        # quitarlo del pool si estaba disponible
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

    # Requests recibidas
    keys_received = r.keys("metrics:*:requests_received")
    for k in keys_received:
        result[k] = int(r.get(k))

    # Requests procesadas
    keys_processed = r.keys("metrics:*:requests_processed")
    for k in keys_processed:
        result[k] = int(r.get(k))

    return result