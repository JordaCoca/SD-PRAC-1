from direct_middleware.redis_client import redis_client
from direct_middleware.config import MAX_TICKETS

# --- UNNUMBERED ---
def buy_unnumbered():
    count = redis_client.incr("tickets_sold")

    if count <= MAX_TICKETS:
        return True
    return False


# --- NUMBERED ---
def buy_numbered(seat_id: int, client_id: str):
    key = f"seat:{seat_id}"

    # SETNX: set if not exists
    result = redis_client.setnx(key, client_id)

    return result == 1