import requests
import time
from concurrent.futures import ThreadPoolExecutor

URL = "http://127.0.0.1:8080/buy"
RESET_URL = "http://127.0.0.1:8080/reset"

NUM_WORKERS = 30

session = requests.Session()


def parse_line(line):
    parts = line.strip().split()
    return {
        "client_id": parts[1],
        "seat_id": int(parts[2]),
        "request_id": parts[3]
    }


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
        requests_list = [parse_line(line) for line in f]

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
    run_benchmark("testing/benchmark_numbered_60000.txt")