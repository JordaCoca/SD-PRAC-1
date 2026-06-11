#!/usr/bin/env python3
"""
SUPER STRESS TEST - Arquitectura directa REST

Tests:
1) Unnumbered no overselling.
2) Numbered duplicados controlados: 1000 requests sobre 100 asientos -> 100 SUCCESS y 900 FAIL.
3) Throughput según workers: 1 vs 2 vs 4 vs 8 vs 16.
4) Contención/hotspot.

Uso:
    python super_stress_test_rest_stable.py

Desde portátil contra PC:
    PowerShell:
        $env:LB_URL="http://IP_DEL_PC:8080"
        python super_stress_test_rest_stable.py
"""

import argparse
import csv
import json
import os
import random
import statistics
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter


LB_URL = os.getenv("LB_URL", "http://127.0.0.1:8080").rstrip("/")
TOTAL_SEATS = int(os.getenv("TOTAL_SEATS", "20000"))
DEFAULT_CLIENT_THREADS = int(os.getenv("CLIENT_THREADS", "32"))
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "10.0"))

RESULT_DIR = Path(os.getenv("RESULT_DIR", "resultados_super_stress_direct"))
RESULT_DIR.mkdir(parents=True, exist_ok=True)

random.seed(42)

_thread_local = threading.local()


def get_session():
    """
    Reutiliza conexiones HTTP por hilo para evitar agotar puertos efímeros en Windows
    cuando se lanzan miles de requests concurrentes.
    """
    session = getattr(_thread_local, "session", None)

    if session is None:
        session = requests.Session()
        adapter = HTTPAdapter(pool_connections=64, pool_maxsize=64, max_retries=0)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        _thread_local.session = session

    return session



def wait_for_lb(max_wait_s=20):
    print(f"Comprobando load balancer: {LB_URL}")
    deadline = time.time() + max_wait_s
    last_error = None

    while time.time() < deadline:
        try:
            r = get_session().get(f"{LB_URL}/metrics", timeout=2)
            if r.status_code < 500:
                print("Load balancer OK.")
                return True
        except Exception as e:
            last_error = e
        time.sleep(0.5)

    raise RuntimeError(f"No puedo conectar con el load balancer en {LB_URL}. Último error: {last_error}")


def reset_system():
    r = get_session().post(f"{LB_URL}/reset", timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    time.sleep(1.0)


def scale_workers(num_workers):
    print(f"Escalando a {num_workers} workers...")

    last_error = None

    for attempt in range(1, 6):
        try:
            r = get_session().post(f"{LB_URL}/scale", params={"num_workers": num_workers}, timeout=60)
            r.raise_for_status()
            data = r.json()
            time.sleep(3.0)
            return data
        except Exception as e:
            last_error = e
            print(f"   /scale falló intento {attempt}/5: {e}")
            time.sleep(5.0)

    raise RuntimeError(f"No se pudo escalar a {num_workers} workers. Último error: {last_error}")


def buy(client_id, request_id, seat_id):
    payload = {
        "client_id": client_id,
        "request_id": request_id,
        "seat_id": seat_id,
    }

    start = time.time()
    try:
        r = get_session().post(f"{LB_URL}/buy", json=payload, timeout=HTTP_TIMEOUT)
        latency_ms = (time.time() - start) * 1000.0

        try:
            data = r.json()
        except Exception:
            data = {}

        return {
            "request_id": request_id,
            "client_id": client_id,
            "seat_id": seat_id,
            "http_status": r.status_code,
            "status": data.get("status", "FAIL"),
            "reason": data.get("reason", ""),
            "latency_ms": latency_ms,
            "error": "",
        }

    except Exception as e:
        latency_ms = (time.time() - start) * 1000.0
        return {
            "request_id": request_id,
            "client_id": client_id,
            "seat_id": seat_id,
            "http_status": 0,
            "status": "HTTP_ERROR",
            "reason": "",
            "latency_ms": latency_ms,
            "error": str(e),
        }


def workload_unnumbered(total):
    for i in range(total):
        yield {
            "client_id": f"client_{i}",
            "request_id": f"unnumbered_{i}",
            "seat_id": None,
        }


def workload_numbered_mod(total_requests, num_seats):
    for i in range(total_requests):
        yield {
            "client_id": f"client_{i}",
            "request_id": f"numbered_mod_{num_seats}_{i}",
            "seat_id": (i % num_seats) + 1,
        }


def workload_numbered_unique(total_requests):
    for i in range(total_requests):
        yield {
            "client_id": f"client_{i}",
            "request_id": f"numbered_unique_{i}",
            "seat_id": (i % TOTAL_SEATS) + 1,
        }


def workload_hotspot_80_5(total_requests):
    hot_seats = max(1, int(TOTAL_SEATS * 0.05))
    for i in range(total_requests):
        if random.random() < 0.80:
            seat_id = random.randint(1, hot_seats)
        else:
            seat_id = random.randint(hot_seats + 1, TOTAL_SEATS)

        yield {
            "client_id": f"client_{i}",
            "request_id": f"hotspot_80_5_{i}",
            "seat_id": seat_id,
        }


def workload_single_seat(total_requests):
    for i in range(total_requests):
        yield {
            "client_id": f"client_{i}",
            "request_id": f"single_seat_{i}",
            "seat_id": 1,
        }


def percentile(values, p):
    if not values:
        return 0.0

    values = sorted(values)
    k = (len(values) - 1) * (p / 100.0)
    f = int(k)
    c = min(f + 1, len(values) - 1)

    if f == c:
        return values[f]

    return values[f] * (c - k) + values[c] * (k - f)


def analyze_results(results):
    success = sum(1 for r in results if r["status"] == "SUCCESS")
    fail = sum(1 for r in results if r["status"] == "FAIL")
    http_error = sum(1 for r in results if r["status"] == "HTTP_ERROR")

    latencies = [r["latency_ms"] for r in results if r["status"] != "HTTP_ERROR"]

    sold_seats = {}
    duplicate_seats = []

    for r in results:
        if r["status"] == "SUCCESS" and r["seat_id"] is not None:
            seat = r["seat_id"]
            if seat in sold_seats:
                duplicate_seats.append(seat)
            else:
                sold_seats[seat] = 1

    return {
        "success": success,
        "fail": fail,
        "http_error": http_error,
        "unique_sold_seats": len(sold_seats),
        "duplicate_success_seats": len(duplicate_seats),
        "duplicate_examples": duplicate_seats[:10],
        "latency_avg_ms": statistics.mean(latencies) if latencies else 0.0,
        "latency_p50_ms": percentile(latencies, 50),
        "latency_p95_ms": percentile(latencies, 95),
        "latency_p99_ms": percentile(latencies, 99),
    }


def run_workload(name, workload_items, workers, client_threads, save_details=False):
    items = list(workload_items)
    total = len(items)

    print(f"\n--- Ejecutando {name} ---")
    print(f"Workers={workers}, requests={total}, client_threads={client_threads}")

    start = time.time()
    results = []

    with ThreadPoolExecutor(max_workers=client_threads) as executor:
        futures = [
            executor.submit(buy, item["client_id"], item["request_id"], item["seat_id"])
            for item in items
        ]

        done = 0
        step = max(1, total // 10)

        for fut in as_completed(futures):
            results.append(fut.result())
            done += 1

            if done % step == 0:
                print(f"   progress {done}/{total}")

    elapsed_s = time.time() - start
    throughput = total / elapsed_s if elapsed_s > 0 else 0.0
    analysis = analyze_results(results)

    row = {
        "test": name,
        "workers": workers,
        "requests": total,
        "elapsed_s": round(elapsed_s, 4),
        "throughput_req_s": round(throughput, 4),
        "success": analysis["success"],
        "fail": analysis["fail"],
        "http_error": analysis["http_error"],
        "unique_sold_seats": analysis["unique_sold_seats"],
        "duplicate_success_seats": analysis["duplicate_success_seats"],
        "latency_avg_ms": round(analysis["latency_avg_ms"], 4),
        "latency_p50_ms": round(analysis["latency_p50_ms"], 4),
        "latency_p95_ms": round(analysis["latency_p95_ms"], 4),
        "latency_p99_ms": round(analysis["latency_p99_ms"], 4),
        "valid": True,
        "notes": "OK",
    }

    if analysis["duplicate_success_seats"] > 0:
        row["valid"] = False
        row["notes"] = f"ERROR: duplicate sold seats: {analysis['duplicate_examples']}"

    if analysis["http_error"] > 0:
        row["valid"] = False
        row["notes"] = f"ERROR: HTTP errors={analysis['http_error']}"

    if save_details:
        details_path = RESULT_DIR / f"details_{name}_{workers}w_{int(time.time())}.json"
        with open(details_path, "w", encoding="utf-8") as f:
            json.dump(results, f, indent=2)
        row["details_file"] = str(details_path)

    print(
        f"   success={row['success']} fail={row['fail']} "
        f"http_error={row['http_error']} throughput={row['throughput_req_s']} req/s "
        f"duplicates={row['duplicate_success_seats']}"
    )

    return row


def test_unnumbered_no_overselling(workers, requests_count, client_threads):
    print("\n================================================")
    print("TEST 1 - UNNUMBERED NO OVERSELLING")
    print("================================================")

    scale_workers(workers)
    reset_system()

    row = run_workload(
        "unnumbered_no_overselling",
        workload_unnumbered(requests_count),
        workers,
        client_threads,
    )

    expected_success = min(TOTAL_SEATS, requests_count)
    expected_fail = max(0, requests_count - TOTAL_SEATS)

    row["expected_success"] = expected_success
    row["expected_fail"] = expected_fail

    if row["success"] != expected_success or row["fail"] != expected_fail:
        row["valid"] = False
        row["notes"] = (
            f"ERROR: expected success={expected_success}, fail={expected_fail}; "
            f"got success={row['success']}, fail={row['fail']}"
        )

    return row


def test_numbered_mod_duplicates(workers, requests_count, seats_count, client_threads):
    print("\n================================================")
    print("TEST 2 - NUMBERED MOD DUPLICATES")
    print("================================================")
    print(
        f"{requests_count} peticiones sobre {seats_count} asientos. "
        f"Esperado: {seats_count} SUCCESS y {requests_count - seats_count} FAIL."
    )

    scale_workers(workers)
    reset_system()

    row = run_workload(
        f"numbered_mod_{requests_count}_req_{seats_count}_seats",
        workload_numbered_mod(requests_count, seats_count),
        workers,
        client_threads,
        save_details=True,
    )

    expected_success = seats_count
    expected_fail = requests_count - seats_count

    row["expected_success"] = expected_success
    row["expected_fail"] = expected_fail

    if row["success"] != expected_success or row["fail"] != expected_fail:
        row["valid"] = False
        row["notes"] = (
            f"ERROR: expected success={expected_success}, fail={expected_fail}; "
            f"got success={row['success']}, fail={row['fail']}"
        )

    if row["duplicate_success_seats"] != 0:
        row["valid"] = False
        row["notes"] = f"ERROR: duplicate_success_seats={row['duplicate_success_seats']}"

    return row


def test_throughput_vs_workers(worker_counts, requests_count, client_threads):
    print("\n================================================")
    print("TEST 3 - THROUGHPUT VS WORKERS")
    print("================================================")

    rows = []

    for workers in worker_counts:
        scale_workers(workers)
        reset_system()

        row = run_workload(
            "throughput_vs_workers_unnumbered",
            workload_unnumbered(requests_count),
            workers,
            client_threads,
        )

        expected_success = min(TOTAL_SEATS, requests_count)
        expected_fail = max(0, requests_count - TOTAL_SEATS)

        row["expected_success"] = expected_success
        row["expected_fail"] = expected_fail

        if row["success"] != expected_success or row["fail"] != expected_fail:
            row["valid"] = False
            row["notes"] = "ERROR: unexpected success/fail count"

        rows.append(row)

    return rows


def test_contention(workers, requests_count, client_threads):
    print("\n================================================")
    print("TEST 4 - CONTENTION / HOTSPOT")
    print("================================================")

    scenarios = [
        ("contention_normal_20000", lambda: workload_numbered_unique(requests_count)),
        ("contention_hotspot_80_5", lambda: workload_hotspot_80_5(requests_count)),
        ("contention_limited_100", lambda: workload_numbered_mod(requests_count, 100)),
        ("contention_limited_20", lambda: workload_numbered_mod(requests_count, 20)),
        ("contention_single_seat", lambda: workload_single_seat(requests_count)),
    ]

    rows = []
    scale_workers(workers)

    for name, factory in scenarios:
        reset_system()

        row = run_workload(
            name,
            factory(),
            workers,
            client_threads,
        )

        if name == "contention_normal_20000":
            row["expected_success"] = min(TOTAL_SEATS, requests_count)
            row["expected_fail"] = max(0, requests_count - TOTAL_SEATS)
        elif name == "contention_hotspot_80_5":
            row["expected_success"] = "variable"
            row["expected_fail"] = "variable"
        elif name == "contention_limited_100":
            row["expected_success"] = 100
            row["expected_fail"] = requests_count - 100
        elif name == "contention_limited_20":
            row["expected_success"] = 20
            row["expected_fail"] = requests_count - 20
        elif name == "contention_single_seat":
            row["expected_success"] = 1
            row["expected_fail"] = requests_count - 1

        if isinstance(row["expected_success"], int):
            if row["success"] != row["expected_success"] or row["fail"] != row["expected_fail"]:
                row["valid"] = False
                row["notes"] = (
                    f"ERROR: expected success={row['expected_success']}, fail={row['expected_fail']}; "
                    f"got success={row['success']}, fail={row['fail']}"
                )

        if row["duplicate_success_seats"] != 0:
            row["valid"] = False
            row["notes"] = f"ERROR: duplicate_success_seats={row['duplicate_success_seats']}"

        rows.append(row)

    return rows


def write_csv(path, rows):
    if not rows:
        return

    columns = []
    for row in rows:
        for key in row.keys():
            if key not in columns:
                columns.append(key)

    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def write_txt(path, rows, client_threads):
    with open(path, "w", encoding="utf-8") as f:
        f.write("SUPER STRESS TEST - DIRECT REST\n")
        f.write(f"LB_URL={LB_URL}\n")
        f.write(f"TOTAL_SEATS={TOTAL_SEATS}\n")
        f.write(f"CLIENT_THREADS={client_threads}\n\n")

        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False, indent=2))
            f.write("\n\n")


def print_summary(rows):
    print("\n================================================")
    print("RESUMEN FINAL")
    print("================================================")

    for row in rows:
        status = "OK" if row.get("valid") else "ERROR"
        print(
            f"[{status}] {row['test']} | "
            f"workers={row['workers']} | "
            f"req={row['requests']} | "
            f"thr={row['throughput_req_s']} req/s | "
            f"success={row['success']} | "
            f"fail={row['fail']} | "
            f"dup={row['duplicate_success_seats']} | "
            f"p95={row['latency_p95_ms']} ms"
        )
        if not row.get("valid"):
            print(f"      {row.get('notes')}")


def parse_worker_counts(text):
    return [int(x.strip()) for x in text.split(",") if x.strip()]


def main():
    parser = argparse.ArgumentParser()

    parser.add_argument("--mode", choices=["all", "correctness", "throughput", "contention"], default="all")
    parser.add_argument("--workers", default="1,2,4,8,16")
    parser.add_argument("--threads", type=int, default=DEFAULT_CLIENT_THREADS)
    parser.add_argument("--correctness-workers", type=int, default=8)
    parser.add_argument("--contention-workers", type=int, default=16)
    parser.add_argument("--unnumbered-requests", type=int, default=25000)
    parser.add_argument("--duplicate-requests", type=int, default=1000)
    parser.add_argument("--duplicate-seats", type=int, default=100)
    parser.add_argument("--throughput-requests", type=int, default=20000)
    parser.add_argument("--contention-requests", type=int, default=20000)

    args = parser.parse_args()

    client_threads = args.threads
    worker_counts = parse_worker_counts(args.workers)

    fecha = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    rows = []

    print("================================================")
    print("SUPER STRESS TEST - DIRECT REST")
    print("================================================")
    print(f"LB_URL={LB_URL}")
    print(f"TOTAL_SEATS={TOTAL_SEATS}")
    print(f"CLIENT_THREADS={client_threads}")
    print(f"mode={args.mode}")
    print("================================================")

    wait_for_lb()

    if args.mode in ("all", "correctness"):
        rows.append(
            test_unnumbered_no_overselling(
                workers=args.correctness_workers,
                requests_count=args.unnumbered_requests,
                client_threads=client_threads,
            )
        )
        rows.append(
            test_numbered_mod_duplicates(
                workers=args.correctness_workers,
                requests_count=args.duplicate_requests,
                seats_count=args.duplicate_seats,
                client_threads=client_threads,
            )
        )

    if args.mode in ("all", "throughput"):
        rows.extend(
            test_throughput_vs_workers(
                worker_counts=worker_counts,
                requests_count=args.throughput_requests,
                client_threads=client_threads,
            )
        )

    if args.mode in ("all", "contention"):
        rows.extend(
            test_contention(
                workers=args.contention_workers,
                requests_count=args.contention_requests,
                client_threads=client_threads,
            )
        )

    out_csv = RESULT_DIR / f"super_stress_direct_summary_{fecha}.csv"
    out_txt = RESULT_DIR / f"super_stress_direct_summary_{fecha}.txt"

    write_csv(out_csv, rows)
    write_txt(out_txt, rows, client_threads)
    print_summary(rows)

    print("\n================================================")
    print("ARCHIVOS GENERADOS")
    print("================================================")
    print(f"CSV: {out_csv}")
    print(f"TXT: {out_txt}")
    print("================================================")


if __name__ == "__main__":
    main()
