from fastapi import FastAPI
from pydantic import BaseModel
import redis
import os
import time
from typing import Optional

app = FastAPI()

r = redis.Redis(host="localhost", port=6379, decode_responses=True)

MAX_SEATS = 20000
WORKER_ID = os.getenv("WORKER_ID", "worker-unknown")


class BuyRequest(BaseModel):
    client_id: str
    seat_id: Optional[int]
    request_id: str


@app.post("/buy")
async def buy(req: BuyRequest):
    # 1. Registro de entrada
    r.incr(f"metrics:{WORKER_ID}:requests_received")
    success = False

    # --- CASO A: ASIENTO NO NUMERADO (UNNUMBERED) ---
    if req.seat_id is None:
        seat = r.spop("available_seats")

        if seat is None:
            # Agotado: registramos fallo y procesado antes de salir
            r.incr(f"metrics:{WORKER_ID}:fail")
            r.incr(f"metrics:{WORKER_ID}:requests_processed")
            return {"status": "FAIL", "reason": "sold out"}

        # Éxito en spop
        r.set(f"seat:{seat}", req.client_id)
        success = True

    # --- CASO B: ASIENTO NUMERADO (NUMBERED) ---
    else:
        # Validación de rango
        if req.seat_id < 1 or req.seat_id > MAX_SEATS:
            r.incr(f"metrics:{WORKER_ID}:fail")
            r.incr(f"metrics:{WORKER_ID}:requests_processed")
            return {"status": "FAIL", "reason": "invalid seat"}

        key = f"seat:{req.seat_id}"

        # Intentamos reservar el asiento
        if r.setnx(key, req.client_id):
            # Éxito: lo quitamos de la bolsa de disponibles (opcional según tu lógica)
            r.srem("available_seats", req.seat_id)
            success = True
        else:
            # Fallo: el asiento ya tenía dueño
            success = False

    # --- REGISTRO FINAL UNIFICADO ---
    # Esto asegura que el benchmark vea el incremento justo antes de recibir la respuesta HTTP
    if success:
        r.incr(f"metrics:{WORKER_ID}:success")
    else:
        r.incr(f"metrics:{WORKER_ID}:fail")

    r.incr(f"metrics:{WORKER_ID}:requests_processed")

    return {"status": "SUCCESS" if success else "FAIL"}


@app.post("/reset")
def reset():
    r.flushall()
    seats = list(range(1, MAX_SEATS + 1))
    # Usamos pipelines para que Redis sea mucho más rápido al insertar 20k elementos
    pipe = r.pipeline()
    # Dividimos en trozos (chunks) para no bloquear el hilo de Redis
    for i in range(0, len(seats), 5000):
        pipe.sadd("available_seats", *seats[i:i + 5000])
    pipe.execute()

    print(f"System reset: {MAX_SEATS} tickets ready.")
    return {"status": "OK", "message": "System wiped and tickets reloaded"}


@app.get("/metrics")
def metrics():
    result = {}

    for k in r.keys("metrics:*"):
        result[k] = int(r.get(k))

    return result