import requests
import time
from concurrent.futures import ThreadPoolExecutor

URL = "http://127.0.0.1:8080/buy"
RESET_URL = "http://127.0.0.1:8080/reset"

NUM_WORKERS = 25

session = requests.Session()


def parse_line(line):
    parts = line.strip().split()

    # Ignorar comentarios o líneas vacías
    if not parts or parts[0] != "BUY":
        return None

    # BUY c0 123 r0  (numbered)
    if len(parts) == 4:
        return {
            "client_id": parts[1],
            "seat_id": int(parts[2]),
            "request_id": parts[3]
        }

    # BUY c0 r0  (unnumbered)
    elif len(parts) == 3:
        return {
            "client_id": parts[1],
            "seat_id": None,
            "request_id": parts[2]
        }

    # formato inválido
    return None


def send(req):
    try:
        r = session.post(URL, json=req, timeout=3)
        return r.json()
    except:
        return {"status": "FAIL"}


def run_benchmark(file_path):
    # reset sistema
    requests.post(RESET_URL)

    with open(file_path, "r") as f:
        requests_list = [
            req for line in f
            if (req := parse_line(line)) is not None
        ]

    start = time.time()

    with ThreadPoolExecutor(max_workers=NUM_WORKERS) as executor:
        results = list(executor.map(send, requests_list))

    end = time.time()

    success = sum(1 for r in results if r.get("status") == "SUCCESS")
    fail = sum(1 for r in results if r.get("status") == "FAIL")

    total_time = end - start
    throughput = len(results) / total_time

    print("---- RESULTS ----")
    print("Total:", len(results))
    print("SUCCESS:", success)
    print("FAIL:", fail)
    print("Time:", round(total_time, 2), "s")
    print("Throughput:", round(throughput, 2), "req/s")


if __name__ == "__main__":
    #run_benchmark("testing/benchmark_numbered_1000.txt")
    run_benchmark("testing/benchmark_unnumbered_1000.txt")