from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
import redis

# Conexión a Redis
r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

app = FastAPI(title="Ticket Numerados API")


class BuyRequest(BaseModel):
    client_id: str
    seat_id: int
    request_id: str


@app.post("/buy")
def buy_ticket(req: BuyRequest):
    seat_key = f"seat:{req.seat_id}"

    # Intentar reservar asiento: SETNX devuelve True si la clave no existía
    reserved = r.setnx(seat_key, req.client_id)
    if reserved:
        return {"status": "SUCCESS", "seat_id": req.seat_id, "client_id": req.client_id}
    else:
        return {"status": "FAIL", "reason": "Seat already sold", "seat_id": req.seat_id}


@app.post("/reset")
def reset():
    r.flushall()
    return {"status": "OK", "message": "All seats cleared"}