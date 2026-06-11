#!/usr/bin/env python3
"""
Benchmark suite para arquitectura directa REST SIN autoscaler.

Pruebas incluidas:
1) Correctness unnumbered: no overselling.
2) Correctness numbered: no overselling por asiento.
3) Throughput vs número de workers: 1, 2, 4, 8.
4) Hotspot / contención.

Requisitos antes de ejecutar:
- Redis levantado.
- load_balancer.py levantado en LB_URL, por defecto http://127.0.0.1:8080.
- NO ejecutar autoscaler.py mientras uses este benchmark.
- Este benchmark usa /scale del load balancer para fijar el número de workers.

Ejemplos:
    python direct_benchmark_no_autoscaler.py
    python direct_benchmark_no_autoscaler.py --mode correctness
    python direct_benchmark_no_autoscaler.py --mode throughput
    python direct_benchmark_no_autoscaler.py --mode hotspot

Con otra IP:
    LB_URL=http://192.168.1.50:8080 python direct_benchmark_no_autoscaler.py
"""

import argparse
import csv
import os
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path

import requests


# ============================================================
# CONFIGURACIÓN
# ============================================================

LB_URL = os.getenv("LB_URL", "http://127.0.0.1:8080").rstrip("/")
RESULT_DIR = Path(os.getenv("RESULT_DIR", "resultados_direct"))

TOTAL_SEATS = int(os.getenv("TOTAL_SEATS", "20000"))
CLIENT_THREADS = int(os.getenv("CLIENT_THREADS", "64"))
HTTP_TIMEOUT = float(os.getenv("HTTP_TIMEOUT", "8.0"))

RESULT_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# HTTP HELPERS
# ============================================================

def reset_system():
    r = requests.post(f"{LB_URL}/reset", timeout=HTTP_TIMEOUT)
    r.raise_for_status()
    time.sleep(1.0)


def get_metrics():
    try:
        return requests.get(f"{LB_URL}/metrics", timeout=HTTP_TIMEOUT).json()
    except Exception:
        return {"received": 0, "processed": 0, "success": 0, "fail": 0}


def scale_workers(n):
    """
    Tu load_balancer.py define:
        async def scale_rest(num_workers: int)
    FastAPI lo interpreta como query param.
    Por eso usamos /scale?num_workers=N.
    """
    r = requests.post(f"{LB_URL}/scale", params={"num_workers": n}, timeout=40)
    r.raise_for_status()
    time.sleep(2.0)
    return r.json()


def buy(client_id, request_id, seat_id):
    payload = {
        "client_id": client_id,
        "request_id": request_id,
        "seat_id": seat_id,
    }

    try:
        r = requests.post(f"{LB_URL}/buy", json=payload, timeout=HTTP_TIMEOUT)
        data = r.json()
        return {
            "ok_http": True,
            "status": data.get("status", "FAIL"),
            "seat_id": seat_id,
            "reason": data.get("reason", ""),
        }
    except Exception as e:
        return {
            "ok_http": False,
            "status": "HTTP_ERROR",
            "seat_id": seat_id,
            "reason": str(e),
        }


# ============================================================
# WORKLOADS
# ============================================================

def workload_unnumbered(total):
    for i in range(total):
        yield {
            "client_id": f"c_{i}",
            "request_id": f"unnumbered_{i}",
            "seat_id": None,
        }


def workload_numbered_round_robin(total, seats=TOTAL_SEATS):
    for i in range(total):
        yield {
            "client_id": f"c_{i}",
            "request_id": f"numbered_rr_{i}",
            "seat_id": (i % seats) + 1,
        }


def workload_hotspot_80_5(total):
    hot_seats = max(1, int(TOTAL_SEATS * 0.05))  # 1000 si TOTAL_SEATS=20000

    for i in range(total):
        if random.random() < 0.80:
            seat_id = random.randint(1, hot_seats)
        else:
            seat_id = random.randint(hot_seats + 1, TOTAL_SEATS)

        yield {
            "client_id": f"c_{i}",
            "request_id": f"hotspot_80_5_{i}",
            "seat_id": seat_id,
        }


def workload_limited(total, seats):
    for i in range(total):
        yield {
            "client_id": f"c_{i}",
            "request_id": f"limited_{seats}_{i}",
            "seat_id": (i % seats) + 1,
        }


def workload_single_seat(total):
    for i in range(total):
        yield {
            "client_id": f"c_{i}",
            "request_id": f"single_seat_{i}",
            "seat_id": 1,
        }


# ============================================================
# RUNNER
# ============================================================

def run_requests(workload_items, progress=True):
    items = list(workload_items)
    total = len(items)

    start = time.time()
    results = []

    with ThreadPoolExecutor(max_workers=CLIENT_THREADS) as executor:
        futures = [
            executor.submit(
                buy,
                item["client_id"],
                item["request_id"],
                item["seat_id"],
            )
            for item in items
        ]

        done = 0
        step = max(1, total // 10)

        for fut in as_completed(futures):
            results.append(fut.result())
            done += 1

            if progress and done % step == 0:
                print(f"      progress {done}/{total}")

    elapsed = time.time() - start
    throughput = total / elapsed if elapsed > 0 else 0.0

    success = sum(1 for r in results if r["status"] == "SUCCESS")
    fail = sum(1 for r in results if r["status"] == "FAIL")
    http_error = sum(1 for r in results if r["status"] == "HTTP_ERROR")

    return {
        "total": total,
        "elapsed_s": elapsed,
        "throughput": throughput,
        "success": success,
        "fail": fail,
        "http_error": http_error,
        "results": results,
    }


def summarize_numbered_no_oversell(results):
    sold = {}
    duplicates = []

    for r in results:
        if r["status"] == "SUCCESS":
            seat = r["seat_id"]
            if seat in sold:
                duplicates.append(seat)
            else:
                sold[seat] = 1

    return {
        "unique_sold_seats": len(sold),
        "duplicate_success_seats": len(duplicates),
        "duplicate_examples": duplicates[:10],
        "ok_no_oversell": len(duplicates) == 0,
    }


# ============================================================
# TESTS
# ============================================================

def test_unnumbered_no_overselling(workers=8, total_requests=25000):
    print("\n=== TEST 1: UNNUMBERED no overselling ===")
    print(f"Workers={workers}, requests={total_requests}")

    scale_workers(workers)
    reset_system()

    run = run_requests(workload_unnumbered(total_requests))

    expected_success = min(TOTAL_SEATS, total_requests)
    expected_fail = max(0, total_requests - TOTAL_SEATS)

    ok = (
        run["success"] == expected_success
        and run["fail"] == expected_fail
        and run["http_error"] == 0
    )

    return {
        "test": "unnumbered_no_overselling",
        "scenario": "unnumbered",
        "workers": workers,
        "requests": total_requests,
        "elapsed_s": round(run["elapsed_s"], 4),
        "throughput": round(run["throughput"], 4),
        "success": run["success"],
        "fail": run["fail"],
        "http_error": run["http_error"],
        "expected_success": expected_success,
        "expected_fail": expected_fail,
        "valid": ok,
        "notes": "OK" if ok else "ERROR: unnumbered overselling or unexpected result",
    }


def test_numbered_no_overselling(workers=8, total_requests=40000):
    print("\n=== TEST 2: NUMBERED no overselling ===")
    print(f"Workers={workers}, requests={total_requests}")

    scale_workers(workers)
    reset_system()

    run = run_requests(workload_numbered_round_robin(total_requests, TOTAL_SEATS))
    check = summarize_numbered_no_oversell(run["results"])

    expected_success = min(TOTAL_SEATS, total_requests)

    ok = (
        check["ok_no_oversell"]
        and run["success"] == expected_success
        and run["success"] <= TOTAL_SEATS
        and run["http_error"] == 0
    )

    return {
        "test": "numbered_no_overselling",
        "scenario": "numbered_round_robin",
        "workers": workers,
        "requests": total_requests,
        "elapsed_s": round(run["elapsed_s"], 4),
        "throughput": round(run["throughput"], 4),
        "success": run["success"],
        "fail": run["fail"],
        "http_error": run["http_error"],
        "expected_success": expected_success,
        "unique_sold_seats": check["unique_sold_seats"],
        "duplicate_success_seats": check["duplicate_success_seats"],
        "valid": ok,
        "notes": "OK" if ok else f"ERROR: duplicates={check['duplicate_examples']}",
    }


def test_throughput_vs_workers(worker_counts=(1, 2, 4, 8), workload_size=12000):
    print("\n=== TEST 3: Throughput vs workers ===")
    rows = []

    for workers in worker_counts:
        print(f"\n--- workers={workers}, workload={workload_size} unnumbered ---")

        scale_workers(workers)
        reset_system()

        run = run_requests(workload_unnumbered(workload_size))

        ok = run["success"] == min(TOTAL_SEATS, workload_size) and run["http_error"] == 0

        rows.append({
            "test": "throughput_vs_workers",
            "scenario": "unnumbered",
            "workers": workers,
            "requests": workload_size,
            "elapsed_s": round(run["elapsed_s"], 4),
            "throughput": round(run["throughput"], 4),
            "success": run["success"],
            "fail": run["fail"],
            "http_error": run["http_error"],
            "valid": ok,
            "notes": "OK" if ok else "Unexpected result",
        })

    return rows


def test_hotspot(workers=12, workload_size=20000):
    print("\n=== TEST 4: Hotspot / contention ===")

    scenarios = [
        ("normal_20000", lambda: workload_numbered_round_robin(workload_size, TOTAL_SEATS)),
        ("hotspot_80_5", lambda: workload_hotspot_80_5(workload_size)),
        ("limited_2000", lambda: workload_limited(workload_size, 2000)),
        ("limited_200", lambda: workload_limited(workload_size, 200)),
        ("limited_20", lambda: workload_limited(workload_size, 20)),
        ("single_seat", lambda: workload_single_seat(workload_size)),
    ]

    rows = []
    scale_workers(workers)

    for name, factory in scenarios:
        print(f"\n--- scenario={name}, workers={workers}, workload={workload_size} ---")

        reset_system()
        run = run_requests(factory())
        check = summarize_numbered_no_oversell(run["results"])

        ok = check["ok_no_oversell"] and run["http_error"] == 0

        rows.append({
            "test": "hotspot",
            "scenario": name,
            "workers": workers,
            "requests": workload_size,
            "elapsed_s": round(run["elapsed_s"], 4),
            "throughput": round(run["throughput"], 4),
            "success": run["success"],
            "fail": run["fail"],
            "http_error": run["http_error"],
            "unique_sold_seats": check["unique_sold_seats"],
            "duplicate_success_seats": check["duplicate_success_seats"],
            "valid": ok,
            "notes": "OK" if ok else f"ERROR: duplicates={check['duplicate_examples']}",
        })

    return rows


# ============================================================
# OUTPUT
# ============================================================

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


def write_txt(path, rows):
    with open(path, "w", encoding="utf-8") as f:
        f.write("DIRECT REST BENCHMARK SIN AUTOSCALER\n")
        f.write(f"LB_URL={LB_URL}\n")
        f.write(f"TOTAL_SEATS={TOTAL_SEATS}\n")
        f.write(f"CLIENT_THREADS={CLIENT_THREADS}\n\n")

        for row in rows:
            f.write(str(row) + "\n")


def print_rows(rows):
    for row in rows:
        print(row)


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser()

    parser.add_argument(
        "--mode",
        choices=["all", "correctness", "throughput", "hotspot"],
        default="all",
        help="Qué bloque ejecutar."
    )

    parser.add_argument("--threads", type=int, default=CLIENT_THREADS)
    parser.add_argument("--hotspot-workers", type=int, default=12)
    parser.add_argument("--hotspot-workload", type=int, default=20000)

    args = parser.parse_args()

    global CLIENT_THREADS
    CLIENT_THREADS = args.threads

    fecha = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    rows = []

    print("================================================")
    print("DIRECT REST BENCHMARK SIN AUTOSCALER")
    print("================================================")
    print(f"LB_URL={LB_URL}")
    print(f"TOTAL_SEATS={TOTAL_SEATS}")
    print(f"CLIENT_THREADS={CLIENT_THREADS}")
    print(f"mode={args.mode}")
    print("================================================")

    if args.mode in ("all", "correctness"):
        rows.append(test_unnumbered_no_overselling(workers=8, total_requests=25000))
        rows.append(test_numbered_no_overselling(workers=8, total_requests=40000))

    if args.mode in ("all", "throughput"):
        rows.extend(test_throughput_vs_workers(worker_counts=(1, 2, 4, 8), workload_size=12000))

    if args.mode in ("all", "hotspot"):
        rows.extend(test_hotspot(workers=args.hotspot_workers, workload_size=args.hotspot_workload))

    out_csv = RESULT_DIR / f"direct_no_autoscaler_summary_{fecha}.csv"
    out_txt = RESULT_DIR / f"direct_no_autoscaler_summary_{fecha}.txt"

    write_csv(out_csv, rows)
    write_txt(out_txt, rows)

    print("\n================================================")
    print("RESULTADOS")
    print("================================================")
    print_rows(rows)
    print("================================================")
    print(f"CSV: {out_csv}")
    print(f"TXT: {out_txt}")
    print("================================================")


if __name__ == "__main__":
    main()
