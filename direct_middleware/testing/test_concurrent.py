import requests
from concurrent.futures import ThreadPoolExecutor

URL = "http://127.0.0.1:8000/buy"

# 🔹 Reutilizamos la sesión para todos los requests
session = requests.Session()

def send(i):
    r = session.post(URL, json={
        "client_id": "c1",
        "request_id": str(i)
    })
    return r.json()

NUM_REQUESTS = 25000
NUM_WORKERS = 100

with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
    results = list(executor.map(send, range(NUM_REQUESTS)))

success = sum(1 for r in results if r["status"] == "success")
fail = sum(1 for r in results if r["status"] == "fail")

print("SUCCESS:", success)
print("FAIL:", fail)