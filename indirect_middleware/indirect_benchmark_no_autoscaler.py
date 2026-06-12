#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Benchmark indirecto RabbitMQ + Redis + workers MQ, sin autoscaler.

Pensado para ejecutarse desde la VM cliente y controlar la torre remota:
  - REST /reset, /scale y /metrics en el load_balancer de la torre
  - RabbitMQ remoto para publicar mensajes
  - Redis remoto para validar duplicados leyendo seat:*

Tests incluidos:
  1) unnumbered_no_overselling
  2) numbered_mod_1000_req_100_seats
  3) throughput_vs_workers_unnumbered para 1,2,3,4,8,16 workers
  4) contention normal/hotspot/limited/single_seat

Uso típico:
  source ~/benchmark/.venv/bin/activate
  pip install pika requests redis
  python indirect_benchmark_no_autoscaler.py --tower-ip 100.81.42.52

Requisitos en la torre:
  docker start redis-server
  docker start rabbitmq-server
  uvicorn load_balancer:app --host 0.0.0.0 --port 8080
"""

import argparse
import csv
import json
import os
import random
import time
from datetime import datetime
from typing import Dict, List, Optional, Tuple

import pika
import requests
import redis


TOTAL_SEATS = 20000
QUEUE_NAME = "ticket_queue"
RESULT_DIR = "resultados_indirect_no_autoscaler"

RABBIT_USER = "admin"
RABBIT_PASS = "superpassword"
RABBIT_PORT = 5672
REDIS_PORT = 6379

DEFAULT_WORKER_LIST = [1, 2, 3, 4, 8, 16]


class RemoteSystem:
    def __init__(self, tower_ip: str, rest_port: int = 8080):
        self.tower_ip = tower_ip
        self.lb_url = f"http://{tower_ip}:{rest_port}"

        self.rabbit_credentials = pika.PlainCredentials(RABBIT_USER, RABBIT_PASS)
        self.rabbit_params = pika.ConnectionParameters(
            host=tower_ip,
            port=RABBIT_PORT,
            virtual_host="/",
            credentials=self.rabbit_credentials,
            heartbeat=600,
            blocked_connection_timeout=300,
        )

        self.redis = redis.Redis(
            host=tower_ip,
            port=REDIS_PORT,
            decode_responses=True,
            socket_timeout=10,
            socket_connect_timeout=10,
        )

    def reset(self) -> None:
        print(f"[RESET] POST {self.lb_url}/reset")
        r = requests.post(f"{self.lb_url}/reset", timeout=30)
        print(f"[RESET] status={r.status_code} body={r.text[:200]}")
        r.raise_for_status()
        time.sleep(1.0)

    def scale(self, workers: int) -> None:
        print(f"[SCALE] workers={workers}")
        r = requests.post(f"{self.lb_url}/scale?num_workers={workers}", timeout=30)
        print(f"[SCALE] status={r.status_code} body={r.text[:200]}")
        r.raise_for_status()
        time.sleep(2.0)

    def metrics(self) -> Dict:
        try:
            return requests.get(f"{self.lb_url}/metrics", timeout=10).json()
        except Exception as e:
            print(f"[WARN] metrics failed: {e}")
            return {"received": 0, "processed": 0, "success": 0, "fail": 0, "active_workers": 0}

    def rabbit_connection(self):
        return pika.BlockingConnection(self.rabbit_params)

    def queue_state(self) -> Tuple[int, int]:
        conn = self.rabbit_connection()
        ch = conn.channel()
        q = ch.queue_declare(queue=QUEUE_NAME, durable=True)
        pending = q.method.message_count
        consumers = q.method.consumer_count
        conn.close()
        return pending, consumers

    def redis_ping(self) -> None:
        self.redis.ping()

    def count_sold_seats(self) -> int:
        count = 0
        for _ in self.redis.scan_iter(match="seat:*", count=1000):
            count += 1
        return count

    def stop_workers(self) -> None:
        try:
            self.scale(0)
        except Exception as e:
            print(f"[WARN] No se pudo hacer scale(0): {e}")


def build_unnumbered_messages(total: int, prefix: str) -> List[Dict]:
    return [
        {"client_id": f"{prefix}_client_{i}", "seat_id": None, "request_id": f"{prefix}_req_{i}"}
        for i in range(total)
    ]


def build_numbered_mod_messages(total: int, num_seats: int, prefix: str) -> List[Dict]:
    return [
        {"client_id": f"{prefix}_client_{i}", "seat_id": (i % num_seats) + 1, "request_id": f"{prefix}_req_{i}"}
        for i in range(total)
    ]


def build_contention_messages(total: int, scenario: str, prefix: str) -> List[Dict]:
    messages = []
    hot_seats = int(TOTAL_SEATS * 0.05)

    for i in range(total):
        if scenario == "normal_20000":
            seat_id = (i % TOTAL_SEATS) + 1
        elif scenario == "hotspot_80_5":
            if random.random() < 0.80:
                seat_id = random.randint(1, hot_seats)
            else:
                seat_id = random.randint(hot_seats + 1, TOTAL_SEATS)
        elif scenario == "limited_100":
            seat_id = (i % 100) + 1
        elif scenario == "limited_20":
            seat_id = (i % 20) + 1
        elif scenario == "single_seat":
            seat_id = 1
        else:
            raise ValueError(f"Unknown contention scenario: {scenario}")

        messages.append(
            {"client_id": f"{prefix}_client_{i}", "seat_id": seat_id, "request_id": f"{prefix}_req_{i}"}
        )

    return messages


def publish_messages(system: RemoteSystem, messages: List[Dict], batch_log: int = 5000) -> float:
    conn = system.rabbit_connection()
    ch = conn.channel()
    ch.queue_declare(queue=QUEUE_NAME, durable=True)
    props = pika.BasicProperties(delivery_mode=2)

    start = time.time()

    for i, msg in enumerate(messages, start=1):
        ch.basic_publish(
            exchange="",
            routing_key=QUEUE_NAME,
            body=json.dumps(msg),
            properties=props,
        )

        if batch_log and i % batch_log == 0:
            print(f"  published {i}/{len(messages)}")

    conn.close()
    return time.time() - start


def wait_until_processed(system: RemoteSystem, expected_processed: int, timeout: float = 300.0, poll_interval: float = 0.5):
    start = time.time()
    samples = []

    while True:
        m = system.metrics()
        pending, consumers = system.queue_state()

        processed = int(m.get("processed", 0))
        success = int(m.get("success", 0))
        fail = int(m.get("fail", 0))

        row = {
            "t_s": round(time.time() - start, 3),
            "processed": processed,
            "success": success,
            "fail": fail,
            "queue_pending": pending,
            "queue_consumers": consumers,
            "metrics_active_workers": m.get("active_workers", 0),
        }
        samples.append(row)

        print(
            f"  [WAIT] processed={processed}/{expected_processed} "
            f"success={success} fail={fail} queue={pending} consumers={consumers}"
        )

        if processed >= expected_processed:
            return time.time() - start, m, samples

        if time.time() - start > timeout:
            print("[WARN] Timeout waiting for processing.")
            return time.time() - start, m, samples

        time.sleep(poll_interval)


def validate_result(test_name, system, requests_count, metrics, expected_success, expected_fail, numbered):
    success = int(metrics.get("success", 0))
    fail = int(metrics.get("fail", 0))
    processed = int(metrics.get("processed", 0))

    unique_sold_seats = system.count_sold_seats() if numbered else 0
    duplicate_success_seats = 0

    notes = []
    ok = True

    if processed != requests_count:
        ok = False
        notes.append(f"processed={processed}, expected={requests_count}")

    if expected_success is not None and success != expected_success:
        ok = False
        notes.append(f"success={success}, expected={expected_success}")

    if expected_fail is not None and fail != expected_fail:
        ok = False
        notes.append(f"fail={fail}, expected={expected_fail}")

    if success + fail != processed:
        ok = False
        notes.append(f"success+fail={success+fail}, processed={processed}")

    if numbered:
        duplicate_success_seats = max(0, success - unique_sold_seats)
        if duplicate_success_seats != 0:
            ok = False
            notes.append(f"duplicate_success_seats={duplicate_success_seats}")

        if unique_sold_seats != success:
            ok = False
            notes.append(f"unique_sold_seats={unique_sold_seats}, success={success}")

        if success > TOTAL_SEATS:
            ok = False
            notes.append("success > TOTAL_SEATS")

    if not notes:
        notes.append("OK")

    return ok, unique_sold_seats, duplicate_success_seats, "; ".join(notes)


def run_case(system, test_name, workers, messages, expected_success, expected_fail, numbered, timeout=300.0):
    print("\n" + "=" * 64)
    print(f"TEST: {test_name} | workers={workers} | requests={len(messages)}")
    print("=" * 64)

    system.reset()
    system.scale(workers)

    start_total = time.time()
    publish_time = publish_messages(system, messages)
    processing_time, metrics, samples = wait_until_processed(system, len(messages), timeout=timeout)
    elapsed_total = time.time() - start_total

    ok, unique_sold, dup, notes = validate_result(
        test_name=test_name,
        system=system,
        requests_count=len(messages),
        metrics=metrics,
        expected_success=expected_success,
        expected_fail=expected_fail,
        numbered=numbered,
    )

    throughput = len(messages) / elapsed_total if elapsed_total > 0 else 0.0

    result = {
        "test": test_name,
        "workers": workers,
        "requests": len(messages),
        "elapsed_s": round(elapsed_total, 4),
        "publish_time_s": round(publish_time, 4),
        "processing_wait_s": round(processing_time, 4),
        "throughput_msg_s": round(throughput, 4),
        "success": int(metrics.get("success", 0)),
        "fail": int(metrics.get("fail", 0)),
        "processed": int(metrics.get("processed", 0)),
        "http_error": 0,
        "unique_sold_seats": unique_sold,
        "duplicate_success_seats": dup,
        "queue_final_pending": system.queue_state()[0],
        "valid": ok,
        "notes": notes,
        "expected_success": expected_success if expected_success is not None else "variable",
        "expected_fail": expected_fail if expected_fail is not None else "variable",
    }

    status = "OK" if ok else "FAIL"
    print(
        f"[{status}] {test_name} | workers={workers} | "
        f"req={len(messages)} | thr={throughput:.2f} msg/s | "
        f"success={result['success']} | fail={result['fail']} | dup={dup} | notes={notes}"
    )

    for s in samples:
        s["test"] = test_name
        s["workers"] = workers

    return result, samples


def parse_workers(value: str) -> List[int]:
    return [int(x.strip()) for x in value.split(",") if x.strip()]


def run_benchmark(args):
    os.makedirs(RESULT_DIR, exist_ok=True)

    system = RemoteSystem(args.tower_ip, rest_port=args.rest_port)

    print("=" * 64)
    print("INDIRECT RABBITMQ BENCHMARK - NO AUTOSCALER")
    print("=" * 64)
    print(f"TOWER_IP={args.tower_ip}")
    print(f"LB_URL={system.lb_url}")
    print(f"RabbitMQ={args.tower_ip}:{RABBIT_PORT}")
    print(f"Redis={args.tower_ip}:{REDIS_PORT}")
    print(f"workers={args.workers}")
    print("=" * 64)

    system.redis_ping()
    pending, consumers = system.queue_state()
    print(f"[CHECK] Redis OK, RabbitMQ OK, queue_pending={pending}, consumers={consumers}")

    stamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    summary_csv = os.path.join(RESULT_DIR, f"indirect_no_autoscaler_summary_{stamp}.csv")
    summary_txt = os.path.join(RESULT_DIR, f"indirect_no_autoscaler_summary_{stamp}.txt")
    samples_csv = os.path.join(RESULT_DIR, f"indirect_no_autoscaler_samples_{stamp}.csv")
    details_json = os.path.join(RESULT_DIR, f"indirect_no_autoscaler_details_{stamp}.json")

    all_results = []
    all_samples = []

    total_un = args.quick_unnumbered if args.quick else 25000
    msgs = build_unnumbered_messages(total_un, prefix=f"unnumbered_{stamp}")
    expected_success = min(TOTAL_SEATS, total_un)
    expected_fail = max(0, total_un - TOTAL_SEATS)

    result, samples = run_case(
        system=system,
        test_name="unnumbered_no_overselling",
        workers=args.correctness_workers,
        messages=msgs,
        expected_success=expected_success,
        expected_fail=expected_fail,
        numbered=False,
        timeout=args.timeout,
    )
    all_results.append(result)
    all_samples.extend(samples)

    total_num = 1000 if not args.quick else 200
    num_seats = 100 if not args.quick else 20
    msgs = build_numbered_mod_messages(total=total_num, num_seats=num_seats, prefix=f"numbered_mod_{stamp}")

    result, samples = run_case(
        system=system,
        test_name=f"numbered_mod_{total_num}_req_{num_seats}_seats",
        workers=args.correctness_workers,
        messages=msgs,
        expected_success=num_seats,
        expected_fail=total_num - num_seats,
        numbered=True,
        timeout=args.timeout,
    )
    all_results.append(result)
    all_samples.extend(samples)

    scale_requests = args.quick_scale_requests if args.quick else args.scale_requests

    for w in args.workers:
        msgs = build_unnumbered_messages(total=scale_requests, prefix=f"scale_{w}w_{stamp}")

        result, samples = run_case(
            system=system,
            test_name="throughput_vs_workers_unnumbered",
            workers=w,
            messages=msgs,
            expected_success=scale_requests,
            expected_fail=0,
            numbered=False,
            timeout=args.timeout,
        )
        all_results.append(result)
        all_samples.extend(samples)

    contention_requests = args.quick_contention_requests if args.quick else args.contention_requests
    contention_scenarios = [
        ("contention_normal_20000", "normal_20000", min(TOTAL_SEATS, contention_requests), max(0, contention_requests - TOTAL_SEATS)),
        ("contention_hotspot_80_5", "hotspot_80_5", None, None),
        ("contention_limited_100", "limited_100", 100, contention_requests - 100),
        ("contention_limited_20", "limited_20", 20, contention_requests - 20),
        ("contention_single_seat", "single_seat", 1, contention_requests - 1),
    ]

    for test_name, scenario, exp_succ, exp_fail in contention_scenarios:
        msgs = build_contention_messages(total=contention_requests, scenario=scenario, prefix=f"{scenario}_{stamp}")

        result, samples = run_case(
            system=system,
            test_name=test_name,
            workers=args.contention_workers,
            messages=msgs,
            expected_success=exp_succ,
            expected_fail=exp_fail,
            numbered=True,
            timeout=args.timeout,
        )
        all_results.append(result)
        all_samples.extend(samples)

    if not args.keep_workers:
        system.stop_workers()

    summary_fields = [
        "test", "workers", "requests", "elapsed_s", "publish_time_s", "processing_wait_s",
        "throughput_msg_s", "success", "fail", "processed", "http_error",
        "unique_sold_seats", "duplicate_success_seats", "queue_final_pending",
        "valid", "notes", "expected_success", "expected_fail",
    ]

    with open(summary_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=summary_fields)
        writer.writeheader()
        writer.writerows(all_results)

    sample_fields = [
        "test", "workers", "t_s", "processed", "success", "fail",
        "queue_pending", "queue_consumers", "metrics_active_workers",
    ]

    with open(samples_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=sample_fields)
        writer.writeheader()
        writer.writerows(all_samples)

    with open(details_json, "w", encoding="utf-8") as f:
        json.dump(all_results, f, indent=2)

    with open(summary_txt, "w", encoding="utf-8") as f:
        f.write("INDIRECT RABBITMQ BENCHMARK - NO AUTOSCALER\n")
        f.write(f"TOWER_IP={args.tower_ip}\n")
        f.write(f"LB_URL={system.lb_url}\n")
        f.write(f"TOTAL_SEATS={TOTAL_SEATS}\n")
        f.write(f"WORKERS={args.workers}\n\n")

        for r in all_results:
            f.write(json.dumps(r, indent=2, ensure_ascii=False))
            f.write("\n\n")

        f.write("=" * 64 + "\n")
        f.write("RESUMEN FINAL\n")
        f.write("=" * 64 + "\n")

        for r in all_results:
            status = "OK" if r["valid"] else "FAIL"
            f.write(
                f"[{status}] {r['test']} | workers={r['workers']} | "
                f"req={r['requests']} | thr={r['throughput_msg_s']} msg/s | "
                f"success={r['success']} | fail={r['fail']} | "
                f"dup={r['duplicate_success_seats']} | notes={r['notes']}\n"
            )

    print("\n" + "=" * 64)
    print("RESUMEN FINAL")
    print("=" * 64)

    for r in all_results:
        status = "OK" if r["valid"] else "FAIL"
        print(
            f"[{status}] {r['test']} | workers={r['workers']} | "
            f"req={r['requests']} | thr={r['throughput_msg_s']} msg/s | "
            f"success={r['success']} | fail={r['fail']} | "
            f"dup={r['duplicate_success_seats']} | notes={r['notes']}"
        )

    print("\n" + "=" * 64)
    print("ARCHIVOS GENERADOS")
    print("=" * 64)
    print(f"Summary CSV: {summary_csv}")
    print(f"Samples CSV: {samples_csv}")
    print(f"Details JSON: {details_json}")
    print(f"Summary TXT: {summary_txt}")


def parse_args():
    parser = argparse.ArgumentParser(description="Benchmark indirecto RabbitMQ sin autoscaler, equivalente al benchmark directo.")

    parser.add_argument("--tower-ip", default="100.81.42.52")
    parser.add_argument("--rest-port", type=int, default=8080)

    parser.add_argument("--workers", type=parse_workers, default=DEFAULT_WORKER_LIST)
    parser.add_argument("--correctness-workers", type=int, default=8)
    parser.add_argument("--contention-workers", type=int, default=16)

    parser.add_argument("--scale-requests", type=int, default=5000)
    parser.add_argument("--contention-requests", type=int, default=20000)

    parser.add_argument("--timeout", type=float, default=300.0)
    parser.add_argument("--keep-workers", action="store_true")

    parser.add_argument("--quick", action="store_true", help="Versión rápida para smoke test.")
    parser.add_argument("--quick-unnumbered", type=int, default=1000)
    parser.add_argument("--quick-scale-requests", type=int, default=1000)
    parser.add_argument("--quick-contention-requests", type=int, default=1000)

    return parser.parse_args()


if __name__ == "__main__":
    random.seed(42)
    run_benchmark(parse_args())
