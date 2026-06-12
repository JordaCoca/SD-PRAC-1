#!/usr/bin/env python3
"""
direct_autoscaler_realistic_benchmark.py

Benchmark para validar el autoscaler de la arquitectura REST directa.

Escenario pensado para workers en modo realistic con delay de 20 ms:
    capacidad teorica aproximada = 1 / 0.020 = 50 req/s por worker

Fases por defecto:
    1) idle inicial
    2) carga alta, objetivo ~4 workers
    3) idle para ver desescalado a 1/0
    4) carga pequena, objetivo ~2 workers
    5) idle final para ver desescalado a 1/0

IMPORTANTE:
    Este benchmark NO llama a /scale. La idea es que el autoscaler sea quien cree/destruya workers.

Uso tipico desde la VM cliente:
    export LB_URL="http://100.81.42.52:8080"
    python direct_autoscaler_realistic_benchmark.py
"""

import argparse
import csv
import json
import math
import os
import queue
import statistics
import threading
import time
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional

import requests

DEFAULT_LB_URL = os.getenv("LB_URL", "http://127.0.0.1:8080")
RESULT_DIR = Path("resultados_direct_autoscaler")
DEFAULT_TARGET_RPS_PER_WORKER = 50.0
DEFAULT_SAMPLE_INTERVAL = 0.5
REQUEST_TIMEOUT = 10.0

_thread_local = threading.local()


def get_session() -> requests.Session:
    if not hasattr(_thread_local, "session"):
        s = requests.Session()
        adapter = requests.adapters.HTTPAdapter(pool_connections=128, pool_maxsize=128, max_retries=0)
        s.mount("http://", adapter)
        s.mount("https://", adapter)
        _thread_local.session = s
    return _thread_local.session


def post_json(url: str, payload: Dict, timeout: float = REQUEST_TIMEOUT) -> Optional[Dict]:
    try:
        r = get_session().post(url, json=payload, timeout=timeout)
        return r.json()
    except Exception:
        return None


def get_json(url: str, timeout: float = 3.0) -> Dict:
    try:
        r = requests.get(url, timeout=timeout)
        return r.json()
    except Exception:
        return {}


def reset_system(lb_url: str) -> None:
    print(f"[RESET] POST {lb_url}/reset")
    try:
        r = requests.post(f"{lb_url}/reset", timeout=10)
        print(f"[RESET] status={r.status_code} body={r.text[:200]}")
    except Exception as e:
        print(f"[WARN] No se pudo llamar a /reset: {e}")
    time.sleep(2)


def normalize_metrics(raw: Dict) -> Dict:
    received = int(raw.get("received", 0) or 0)
    processed = int(raw.get("processed", 0) or 0)
    success = int(raw.get("success", 0) or 0)
    fail = int(raw.get("fail", 0) or 0)
    pending = int(raw.get("pending", max(0, received - processed)) or 0)

    active_workers = raw.get("active_workers", "")
    if active_workers == "" and isinstance(raw.get("workers"), list):
        active_workers = len(raw["workers"])

    return {
        "received": received,
        "processed": processed,
        "success": success,
        "fail": fail,
        "pending": pending,
        "active_workers": active_workers,
    }


def sample_state(lb_url: str, phase_name: str, global_start: float, phase_target_rps: float,
                 target_rps_per_worker: float, prev_sample: Optional[Dict]) -> Dict:
    now = time.time()
    raw = get_json(f"{lb_url}/metrics")
    m = normalize_metrics(raw)

    if prev_sample is None:
        observed_rps = 0.0
    else:
        dt = max(1e-9, now - prev_sample["sample_time"])
        delta_processed = max(0, m["processed"] - prev_sample["processed"])
        observed_rps = delta_processed / dt

    if phase_target_rps <= 0:
        expected_workers = 1
    else:
        expected_workers = max(1, math.ceil(phase_target_rps / target_rps_per_worker))

    return {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "sample_time": now,
        "t_rel_s": round(now - global_start, 2),
        "phase": phase_name,
        "target_rps": phase_target_rps,
        "expected_workers_by_target": expected_workers,
        "active_workers_reported": m["active_workers"],
        "received": m["received"],
        "processed": m["processed"],
        "success": m["success"],
        "fail": m["fail"],
        "pending": m["pending"],
        "observed_processed_rps": round(observed_rps, 2),
    }


def make_buy_payload(i: int, phase_name: str, mode: str = "unnumbered") -> Dict:
    if mode == "unnumbered":
        seat_id = None
    elif mode == "numbered":
        seat_id = (i % 20000) + 1
    else:
        raise ValueError(f"mode desconocido: {mode}")

    return {
        "client_id": f"autoscaler_direct_{phase_name}_{i}",
        "seat_id": seat_id,
        "request_id": f"autoscaler_direct_{phase_name}_{int(time.time() * 1000)}_{i}",
    }


def worker_sender(lb_url: str, task_queue: "queue.Queue[Optional[Dict]]", results: List[Dict], lock: threading.Lock):
    while True:
        item = task_queue.get()
        if item is None:
            task_queue.task_done()
            break

        idx = item["idx"]
        phase_name = item["phase_name"]
        mode = item["mode"]
        t0 = time.perf_counter()
        payload = make_buy_payload(idx, phase_name, mode=mode)
        response = post_json(f"{lb_url}/buy", payload)
        latency_ms = (time.perf_counter() - t0) * 1000.0

        if response is None:
            status = "HTTP_ERROR"
        else:
            status = response.get("status", "UNKNOWN")

        with lock:
            results.append({"phase": phase_name, "status": status, "latency_ms": latency_ms})

        task_queue.task_done()


def start_sender_threads(lb_url: str, threads: int, results: List[Dict], lock: threading.Lock):
    q: "queue.Queue[Optional[Dict]]" = queue.Queue(maxsize=threads * 8)
    executor = ThreadPoolExecutor(max_workers=threads)
    futures = [executor.submit(worker_sender, lb_url, q, results, lock) for _ in range(threads)]
    return q, executor, futures


def stop_sender_threads(q, executor, futures, threads: int):
    for _ in range(threads):
        q.put(None)
    q.join()
    for f in futures:
        try:
            f.result(timeout=2)
        except Exception:
            pass
    executor.shutdown(wait=True)


def inject_rate_limited(lb_url: str, phase_name: str, rps: float, duration_s: float,
                        client_threads: int, mode: str, results: List[Dict], lock: threading.Lock):
    total = int(rps * duration_s)
    if total <= 0:
        return 0

    print(f"[LOAD] {phase_name}: target_rps={rps}, duration={duration_s}s, total≈{total}, threads={client_threads}")
    q, executor, futures = start_sender_threads(lb_url, client_threads, results, lock)

    start = time.perf_counter()
    for i in range(total):
        next_send = start + (i / rps)
        now = time.perf_counter()
        if next_send > now:
            time.sleep(next_send - now)
        q.put({"idx": i, "phase_name": phase_name, "mode": mode})

    q.join()
    stop_sender_threads(q, executor, futures, client_threads)
    return total


def summarize_phase_results(results: List[Dict], phase_name: str) -> Dict:
    phase_results = [r for r in results if r["phase"] == phase_name]
    latencies = [r["latency_ms"] for r in phase_results if r["status"] != "HTTP_ERROR"]
    success = sum(1 for r in phase_results if r["status"] == "SUCCESS")
    fail = sum(1 for r in phase_results if r["status"] == "FAIL")
    http_error = sum(1 for r in phase_results if r["status"] == "HTTP_ERROR")
    total = len(phase_results)

    if latencies:
        ordered = sorted(latencies)
        avg = statistics.mean(latencies)
        p95 = ordered[int(0.95 * (len(ordered) - 1))]
        p99 = ordered[int(0.99 * (len(ordered) - 1))]
    else:
        avg = p95 = p99 = 0.0

    return {
        "sent_completed": total,
        "success": success,
        "fail": fail,
        "http_error": http_error,
        "lat_avg_ms": round(avg, 3),
        "lat_p95_ms": round(p95, 3),
        "lat_p99_ms": round(p99, 3),
    }


def run_phase(lb_url: str, phase_name: str, target_rps: float, duration_s: float,
              client_threads: int, mode: str, sample_writer, global_start: float,
              target_rps_per_worker: float, sample_interval: float,
              results: List[Dict], results_lock: threading.Lock) -> Dict:
    expected = max(1, math.ceil(target_rps / target_rps_per_worker)) if target_rps > 0 else 1
    print("\n================================================")
    print(f"FASE: {phase_name}")
    print(f"target_rps={target_rps}, duration={duration_s}s, expected_workers≈{expected}")
    print("================================================")

    phase_start_metrics = normalize_metrics(get_json(f"{lb_url}/metrics"))
    phase_start_time = time.time()

    stop_sampling = threading.Event()
    prev_sample = None
    max_pending = 0
    max_reported_workers = 0
    max_observed_rps = 0.0

    def sampler():
        nonlocal prev_sample, max_pending, max_reported_workers, max_observed_rps
        while not stop_sampling.is_set():
            row = sample_state(lb_url, phase_name, global_start, target_rps, target_rps_per_worker, prev_sample)
            prev_sample = row
            max_pending = max(max_pending, int(row["pending"]))
            max_observed_rps = max(max_observed_rps, float(row["observed_processed_rps"]))
            try:
                aw = int(row["active_workers_reported"])
                max_reported_workers = max(max_reported_workers, aw)
            except Exception:
                pass

            sample_writer.writerow({k: v for k, v in row.items() if k != "sample_time"})
            print(
                f"t={row['t_rel_s']:6.1f}s | phase={phase_name:28} | "
                f"target={target_rps:6.1f} rps | obs={row['observed_processed_rps']:6.1f} rps | "
                f"pending={row['pending']:5} | workers={row['active_workers_reported']}"
            )
            time.sleep(sample_interval)

    th = threading.Thread(target=sampler, daemon=True)
    th.start()

    sent = 0
    if target_rps > 0:
        sent = inject_rate_limited(lb_url, phase_name, target_rps, duration_s, client_threads, mode, results, results_lock)
    else:
        time.sleep(duration_s)

    time.sleep(1.0)
    stop_sampling.set()
    th.join(timeout=2)

    phase_end_time = time.time()
    phase_end_metrics = normalize_metrics(get_json(f"{lb_url}/metrics"))
    processed_delta = max(0, phase_end_metrics["processed"] - phase_start_metrics["processed"])
    received_delta = max(0, phase_end_metrics["received"] - phase_start_metrics["received"])
    success_delta = max(0, phase_end_metrics["success"] - phase_start_metrics["success"])
    fail_delta = max(0, phase_end_metrics["fail"] - phase_start_metrics["fail"])
    elapsed = phase_end_time - phase_start_time
    throughput = processed_delta / elapsed if elapsed > 0 else 0.0
    client_summary = summarize_phase_results(results, phase_name)

    summary = {
        "phase": phase_name,
        "target_rps": target_rps,
        "duration_s": duration_s,
        "expected_workers": expected,
        "max_reported_workers": max_reported_workers if max_reported_workers > 0 else "",
        "max_pending": max_pending,
        "max_observed_processed_rps": round(max_observed_rps, 3),
        "sent_intended": sent,
        "client_completed": client_summary["sent_completed"],
        "client_success": client_summary["success"],
        "client_fail": client_summary["fail"],
        "client_http_error": client_summary["http_error"],
        "metrics_received_delta": received_delta,
        "metrics_processed_delta": processed_delta,
        "metrics_success_delta": success_delta,
        "metrics_fail_delta": fail_delta,
        "phase_throughput_processed_rps": round(throughput, 3),
        "lat_avg_ms": client_summary["lat_avg_ms"],
        "lat_p95_ms": client_summary["lat_p95_ms"],
        "lat_p99_ms": client_summary["lat_p99_ms"],
    }
    print("[RESUMEN FASE]", json.dumps(summary, indent=2))
    return summary


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--lb-url", default=DEFAULT_LB_URL)
    parser.add_argument("--client-threads", type=int, default=256)
    parser.add_argument("--target-rps-per-worker", type=float, default=DEFAULT_TARGET_RPS_PER_WORKER)
    parser.add_argument("--sample-interval", type=float, default=DEFAULT_SAMPLE_INTERVAL)
    parser.add_argument("--high-rps", type=float, default=200.0, help="Carga alta: 200 rps ≈ 4 workers con 50 rps/worker")
    parser.add_argument("--small-rps", type=float, default=90.0, help="Carga pequena: 90 rps ≈ 2 workers con 50 rps/worker")
    parser.add_argument("--duration-initial-idle", type=float, default=8.0)
    parser.add_argument("--duration-high", type=float, default=30.0)
    parser.add_argument("--duration-idle-after-high", type=float, default=25.0)
    parser.add_argument("--duration-small", type=float, default=25.0)
    parser.add_argument("--duration-final-idle", type=float, default=25.0)
    parser.add_argument("--request-mode", choices=["unnumbered", "numbered"], default="unnumbered")
    parser.add_argument("--no-reset", action="store_true")
    args = parser.parse_args()

    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    fecha = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    samples_path = RESULT_DIR / f"direct_autoscaler_samples_{fecha}.csv"
    summary_path = RESULT_DIR / f"direct_autoscaler_summary_{fecha}.csv"

    print("================================================")
    print("DIRECT AUTOSCALER REALISTIC BENCHMARK")
    print("================================================")
    print(f"LB_URL={args.lb_url}")
    print(f"target_rps_per_worker={args.target_rps_per_worker}")
    print(f"high_rps={args.high_rps} -> expected≈{math.ceil(args.high_rps / args.target_rps_per_worker)} workers")
    print(f"small_rps={args.small_rps} -> expected≈{math.ceil(args.small_rps / args.target_rps_per_worker)} workers")
    print("\nIMPORTANTE: arranca el autoscaler en la torre con workers realistas, por ejemplo:")
    print('  PowerShell: $env:WORKER_MODE="realistic"; $env:REALISTIC_DELAY_MS="20"; python rest_app/autoscaler.py')
    print("")

    if not args.no_reset:
        reset_system(args.lb_url)

    print(f"[METRICS INICIAL] {normalize_metrics(get_json(f'{args.lb_url}/metrics'))}")

    fieldnames_samples = [
        "timestamp", "t_rel_s", "phase", "target_rps", "expected_workers_by_target",
        "active_workers_reported", "received", "processed", "success", "fail", "pending", "observed_processed_rps",
    ]
    fieldnames_summary = [
        "phase", "target_rps", "duration_s", "expected_workers", "max_reported_workers", "max_pending",
        "max_observed_processed_rps", "sent_intended", "client_completed", "client_success", "client_fail",
        "client_http_error", "metrics_received_delta", "metrics_processed_delta", "metrics_success_delta",
        "metrics_fail_delta", "phase_throughput_processed_rps", "lat_avg_ms", "lat_p95_ms", "lat_p99_ms",
    ]

    phases = [
        ("idle_initial_expect_scale_down", 0.0, args.duration_initial_idle),
        ("high_load_expect_4_workers", args.high_rps, args.duration_high),
        ("idle_after_high_expect_1_or_0", 0.0, args.duration_idle_after_high),
        ("small_load_expect_2_workers", args.small_rps, args.duration_small),
        ("idle_final_expect_1_or_0", 0.0, args.duration_final_idle),
    ]

    global_start = time.time()
    all_results: List[Dict] = []
    results_lock = threading.Lock()
    summaries = []

    with samples_path.open("w", newline="", encoding="utf-8") as f_samples:
        sample_writer = csv.DictWriter(f_samples, fieldnames=fieldnames_samples)
        sample_writer.writeheader()
        for name, rps, duration in phases:
            summaries.append(run_phase(
                lb_url=args.lb_url,
                phase_name=name,
                target_rps=rps,
                duration_s=duration,
                client_threads=args.client_threads,
                mode=args.request_mode,
                sample_writer=sample_writer,
                global_start=global_start,
                target_rps_per_worker=args.target_rps_per_worker,
                sample_interval=args.sample_interval,
                results=all_results,
                results_lock=results_lock,
            ))

    with summary_path.open("w", newline="", encoding="utf-8") as f_summary:
        writer = csv.DictWriter(f_summary, fieldnames=fieldnames_summary)
        writer.writeheader()
        for s in summaries:
            writer.writerow(s)

    print("\n================================================")
    print("BENCHMARK COMPLETADO")
    print("================================================")
    print(f"Samples CSV: {samples_path}")
    print(f"Summary CSV: {summary_path}")
    print("\nInterpretacion rapida:")
    print("- high_load_expect_4_workers deberia forzar una subida cercana a 4 workers si el autoscaler ve backlog/RPS.")
    print("- idle_after_high_expect_1_or_0 deberia mostrar desescalado.")
    print("- small_load_expect_2_workers deberia volver a subir hacia 2 workers.")
    print("- idle_final_expect_1_or_0 deberia volver a desescalar.")
    print("- Si active_workers_reported sale vacio, mira la terminal del autoscaler; tu /metrics no esta exponiendo el numero de workers.")


if __name__ == "__main__":
    main()
