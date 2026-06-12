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

# Modos de ejecución del worker:
# - optimized: comportamiento original, sin retardo artificial.
# - realistic: simula lógica de negocio/BD añadiendo un delay por compra.
WORKER_MODE = os.getenv("WORKER_MODE", "optimized").lower()
REALISTIC_DELAY_MS = float(os.getenv("REALISTIC_DELAY_MS", "20"))

def busy_wait_ms(ms: float):
    end = time.perf_counter() + (ms / 1000.0)
    while time.perf_counter() < end:
        pass

class BuyRequest(BaseModel):
    client_id: str
    seat_id: Optional[int]
    request_id: str


@app.post("/buy")
def buy(req: BuyRequest):
    # 1. Registro de entrada
    r.incr(f"metrics:{WORKER_ID}:requests_received")
    success = False

    # Modo realista: simula trabajo de negocio por petición.
    # Por defecto son 20 ms, configurable con REALISTIC_DELAY_MS.
    if WORKER_MODE in ("realistic", "realista"):
        busy_wait_ms(REALISTIC_DELAY_MS)

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
        lock_key = f"lock:seat:{req.seat_id}"
        seat_key = f"seat:{req.seat_id}"

        # Intentar adquirir un lock (spin-lock manual)
        # Seteamos un tiempo de vida (PX) para evitar deadlocks si el worker muere
        acquired = r.set(lock_key, "locked", nx=True, px=1000)

        if acquired:
            try:
                # Verificar si ya está vendido
                if r.exists(seat_key):
                    success = False
                else:
                    r.set(seat_key, req.client_id)
                    success = True
            finally:
                # Liberar el lock
                r.delete(lock_key)
        else:
            # No se pudo obtener el lock (Contención detectada)
            success = False

    # --- REGISTRO FINAL UNIFICADO ---
    # Esto asegura que el benchmark vea el incremento justo antes de recibir la respuesta HTTP
    if success:
        r.incr(f"metrics:{WORKER_ID}:success")
    else:
        r.incr(f"metrics:{WORKER_ID}:fail")

    r.incr(f"metrics:{WORKER_ID}:requests_processed")

    return {"status": "SUCCESS" if success else "FAIL"}


@app.get("/mode")
def mode():
    return {
        "worker_id": WORKER_ID,
        "mode": WORKER_MODE,
        "realistic_delay_ms": REALISTIC_DELAY_MS if WORKER_MODE in ("realistic", "realista") else 0
    }


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

# Permite levantar un worker directamente con flag, por ejemplo:
#   python -m rest_app.main --port 8001
#   python -m rest_app.main --port 8001 --realistic
#   python -m rest_app.main --port 8001 --mode realistic --delay-ms 20
if __name__ == "__main__":
    import argparse
    import uvicorn

    parser = argparse.ArgumentParser(description="Ticket worker REST")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8001)
    parser.add_argument("--mode", choices=["optimized", "realistic"], default=os.getenv("WORKER_MODE", "optimized"))
    parser.add_argument("--realistic", action="store_true", help="Alias de --mode realistic")
    parser.add_argument("--delay-ms", type=float, default=None, help="Delay del modo realistic en milisegundos")
    args = parser.parse_args()

    selected_mode = "realistic" if args.realistic else args.mode
    os.environ["WORKER_MODE"] = selected_mode

    if args.delay_ms is not None:
        os.environ["REALISTIC_DELAY_MS"] = str(args.delay_ms)

    uvicorn.run("rest_app.main:app", host=args.host, port=args.port)
