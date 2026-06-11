#!/usr/bin/env python3
"""
Benchmark independiente para probar SOLO el autoscaler de la arquitectura directa REST.

Requisitos:
- Redis levantado.
- load_balancer.py levantado en LB_URL.
- autoscaler.py ejecutándose en otra terminal.
- NO ejecutar /scale manualmente mientras corre este test.

Qué hace:
- Resetea el sistema.
- Genera cargas sostenidas de distinto RPS.
- Muestra cuántos workers vivos hay en los puertos 8001..8040.
- Guarda samples y resumen en CSV.

Ejemplo:
    python direct_autoscaler_benchmark.py

Con otra IP del load balancer:
    LB_URL=http://192.168.1.50:8080 python direct_autoscaler_benchmark.py
"""

import argparse
import csv
import os
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

# Autoscaler.py usa BASE_PORT=8001 y MAX_WORKERS=40.
BASE_PORT = int(os.getenv("BASE_PORT", "8001"))
MAX_WORKER_PORTS = int(os.getenv("MAX_WORKER_PORTS", "40"))

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


def buy(client_id, request_id, seat_id):
    payload = {
        "client_id": client_id,
        "request_id": request_id,
        "seat_id": seat_id,
    }

    try:
        r = requests.post(f"{LB_URL}/buy", json=payload, timeout=HTTP_TIMEOUT)
        data = r.json()
        return data.get("status", "FAIL")
    except Exception:
        return "HTTP_ERROR"


def count_live_worker_ports():
    """
    Cuenta workers vivos probando /metrics directamente en puertos 8001..8040.
    Esto asume que ejecutas el benchmark en la misma máquina que los workers.
    Si el autoscaler está en otra máquina, cambia WORKER_HOST.
    """
    worker_host = os.getenv("WORKER_HOST", "127.0.0.1")
    live = []

    for i in range(MAX_WORKER_PORTS):
        port = BASE_PORT + i
        url = f"http://{worker_host}:{port}/metrics"

        try:
            r = requests.get(url, timeout=0.15)
            if r.status_code < 500:
                live.append(port)
        except Exception:
            pass

    return live


# ============================================================
# LOAD GENERATOR
# ============================================================

def send_sustained_load(duration_s, rps, numbered=False):
    """
    Genera tráfico durante duration_s segundos.
    La precisión no es perfecta, pero es suficiente para forzar el autoscaler.
    """
    interval = 1.0 / rps if rps > 0 else 0.0
    end = time.time() + duration_s

    sent = 0
    results = []

    def make_request(i):
        if numbered:
            seat_id = (i % TOTAL_SEATS) + 1
        else:
            seat_id = None

        return buy(
            client_id=f"auto_c_{i}",
            request_id=f"auto_req_{int(time.time())}_{i}",
            seat_id=seat_id,
        )

    with ThreadPoolExecutor(max_workers=CLIENT_THREADS) as executor:
        futures = []

        while time.time() < end:
            futures.append(executor.submit(make_request, sent))
            sent += 1

            if interval > 0:
                time.sleep(interval)

        for fut in as_completed(futures):
            results.append(fut.result())

    return {
        "sent": sent,
        "success": sum(1 for r in results if r == "SUCCESS"),
        "fail": sum(1 for r in results if r == "FAIL"),
        "http_error": sum(1 for r in results if r == "HTTP_ERROR"),
    }


# ============================================================
# AUTOSCALER TEST
# ============================================================

def test_autoscaler(workloads, phase_duration_s, cooldown_s):
    print("\n=== AUTOSCALER BENCHMARK ===")
    print("Asegúrate de tener autoscaler.py corriendo en otra terminal.")
    print(f"LB_URL={LB_URL}")

    reset_system()
    time.sleep(3)

    samples = []
    summary = []

    for target_rps in workloads:
        print(f"\n--- Fase target_rps={target_rps}, duration={phase_duration_s}s ---")

        phase_start = time.time()
        max_workers = 0
        max_pending = 0

        with ThreadPoolExecutor(max_workers=1) as executor:
            load_future = executor.submit(send_sustained_load, phase_duration_s, target_rps, False)

            while not load_future.done():
                live_ports = count_live_worker_ports()
                metrics = get_metrics()
                pending = max(0, metrics.get("received", 0) - metrics.get("processed", 0))

                sample = {
                    "target_rps": target_rps,
                    "t_rel_s": round(time.time() - phase_start, 3),
                    "live_workers": len(live_ports),
                    "live_ports": " ".join(map(str, live_ports)),
                    "received": metrics.get("received", 0),
                    "processed": metrics.get("processed", 0),
                    "success": metrics.get("success", 0),
                    "fail": metrics.get("fail", 0),
                    "pending": pending,
                }

                samples.append(sample)

                max_workers = max(max_workers, len(live_ports))
                max_pending = max(max_pending, pending)

                print(
                    f"      t={sample['t_rel_s']:.1f}s "
                    f"workers={sample['live_workers']} "
                    f"pending={pending} "
                    f"received={sample['received']} "
                    f"processed={sample['processed']}"
                )

                time.sleep(0.5)

            load_result = load_future.result()

        print(f"   Load phase ended: {load_result}")
        print(f"   Esperando cooldown {cooldown_s}s para ver desescalado...")
        time.sleep(cooldown_s)

        live_after = count_live_worker_ports()

        row = {
            "test": "autoscaler",
            "target_rps": target_rps,
            "duration_s": phase_duration_s,
            "sent": load_result["sent"],
            "success": load_result["success"],
            "fail": load_result["fail"],
            "http_error": load_result["http_error"],
            "max_workers_seen": max_workers,
            "workers_after_cooldown": len(live_after),
            "max_pending_seen": max_pending,
            "valid": max_workers >= 1 and load_result["http_error"] == 0,
            "notes": "OK" if load_result["http_error"] == 0 else "HTTP errors detected",
        }

        summary.append(row)

    return samples, summary


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


# ============================================================
# MAIN
# ============================================================

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--workloads", nargs="+", type=int, default=[20, 80, 200, 500])
    parser.add_argument("--duration", type=int, default=8)
    parser.add_argument("--cooldown", type=int, default=6)
    parser.add_argument("--threads", type=int, default=CLIENT_THREADS)

    args = parser.parse_args()

    global CLIENT_THREADS
    CLIENT_THREADS = args.threads

    fecha = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    samples, summary = test_autoscaler(
        workloads=args.workloads,
        phase_duration_s=args.duration,
        cooldown_s=args.cooldown,
    )

    samples_csv = RESULT_DIR / f"direct_autoscaler_samples_{fecha}.csv"
    summary_csv = RESULT_DIR / f"direct_autoscaler_summary_{fecha}.csv"

    write_csv(samples_csv, samples)
    write_csv(summary_csv, summary)

    print("\n================================================")
    print("AUTOSCALER BENCHMARK COMPLETADO")
    print("================================================")
    print(f"Samples CSV: {samples_csv}")
    print(f"Summary CSV: {summary_csv}")
    print("Resumen:")
    for row in summary:
        print(row)
    print("================================================")


if __name__ == "__main__":
    main()
