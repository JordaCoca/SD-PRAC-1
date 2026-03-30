# app/test_concurrent_numbered.py
import requests
from concurrent.futures import ThreadPoolExecutor

URL = "http://127.0.0.1:8000/buy"
NUM_REQUESTS = 1000
NUM_WORKERS = 50
SEAT_ID = 1  # Todos intentan comprar el mismo asiento

success = 0
fail = 0


def send(i):
    global success, fail
    r = requests.post(URL, json={
        "client_id": f"c{i}",
        "seat_id": SEAT_ID,
        "request_id": str(i)
    })
    data = r.json()
    if data["status"] == "SUCCESS":
        success += 1
    else:
        fail += 1


if __name__ == "__main__":
    # Limpiar Redis antes de test
    requests.post("http://127.0.0.1:8000/reset")

    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        executor.map(send, range(NUM_REQUESTS))

    print("SUCCESS:", success)
    print("FAIL:", fail)