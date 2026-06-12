#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
Benchmark para autoscaler indirecto RabbitMQ + workers MQ.

Idea:
  - NO usa /scale. El autoscaler debe estar arrancado en la torre.
  - Publica 3 ráfagas: grande -> pausa -> mediana -> pausa -> pequeña -> pausa.
  - Mide en cada muestra:
      * mensajes pendientes en RabbitMQ
      * consumers reales en la cola ticket_queue
      * workers esperados según la regla del autoscaler: ceil(queue_depth / 500)
      * métricas de Redis leídas por /metrics

Uso en la VM cliente:
  cd ~/benchmark/indirecto
  source ~/benchmark/.venv/bin/activate
  pip install pika requests
  python indirect_autoscaler_burst_benchmark.py --tower-ip 100.81.42.52

Antes, en la torre:
  docker start redis-server
  docker start rabbitmq-server
  uvicorn load_balancer:app --host 0.0.0.0 --port 8080

Y en otra terminal de la torre:
  python mq_autoscaler.py

IMPORTANTE:
  No ejecutes manualmente /scale durante este benchmark.
"""

import argparse
import csv
import json
import math
import os
import time
from datetime import datetime
from typing import Dict, List, Tuple

import pika
import requests


# Debe coincidir con mq_autoscaler.py
QUEUE_NAME = "ticket_queue"
MESSAGES_PER_WORKER = 500
MIN_WORKERS = 1
MAX_WORKERS = 16

RABBIT_USER = "admin"
RABBIT_PASS = "superpassword"
RABBIT_PORT = 5672

RESULT_DIR = "resultados_indirect_autoscaler"


class RemoteMQSystem:
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

    def reset(self) -> None:
        print(f"[RESET] POST {self.lb_url}/reset")
        r = requests.post(f"{self.lb_url}/reset", timeout=30)
        print(f"[RESET] status={r.status_code} body={r.text[:200]}")
        r.raise_for_status()
        time.sleep(2)

    def metrics(self) -> Dict:
        try:
            return requests.get(f"{self.lb_url}/metrics", timeout=5).json()
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


def expected_workers_from_queue(queue_depth: int) -> int:
    desired = math.ceil(queue_depth / MESSAGES_PER_WORKER)
    desired = max(MIN_WORKERS, min(desired, MAX_WORKERS))
    return desired


def publish_burst(system: RemoteMQSystem, total_messages: int, label: str, mode: str = "unnumbered") -> float:
    """
    Publica una ráfaga de mensajes a RabbitMQ.
    Por defecto usa unnumbered porque interesa medir escalado, no contención.
    """
    conn = system.rabbit_connection()
    ch = conn.channel()
    ch.queue_declare(queue=QUEUE_NAME, durable=True)

    props = pika.BasicProperties(delivery_mode=2)

    start = time.time()

    for i in range(total_messages):
        if mode == "unnumbered":
            msg = {
                "client_id": f"{label}_client_{i}",
                "seat_id": None,
                "request_id": f"{label}_req_{int(start)}_{i}",
            }
        elif mode == "numbered":
            msg = {
                "client_id": f"{label}_client_{i}",
                "seat_id": (i % 20000) + 1,
                "request_id": f"{label}_req_{int(start)}_{i}",
            }
        elif mode == "hotspot":
            # 80% de peticiones al 5% de asientos.
            if i % 10 < 8:
                seat_id = (i % 1000) + 1
            else:
                seat_id = 1001 + (i % 19000)
            msg = {
                "client_id": f"{label}_client_{i}",
                "seat_id": seat_id,
                "request_id": f"{label}_req_{int(start)}_{i}",
            }
        else:
            raise ValueError(f"mode desconocido: {mode}")

        ch.basic_publish(
            exchange="",
            routing_key=QUEUE_NAME,
            body=json.dumps(msg),
            properties=props,
        )

    conn.close()
    return time.time() - start


def sample_state(system: RemoteMQSystem, global_start: float, phase: str) -> Dict:
    pending, consumers = system.queue_state()
    metrics = system.metrics()
    expected = expected_workers_from_queue(pending)

    processed = int(metrics.get("processed", 0))
    success = int(metrics.get("success", 0))
    fail = int(metrics.get("fail", 0))
    received = int(metrics.get("received", 0))

    return {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "t_rel_s": round(time.time() - global_start, 3),
        "phase": phase,
        "queue_depth": pending,
        "expected_workers_now": expected,
        "actual_consumers": consumers,
        "metrics_active_workers": metrics.get("active_workers", 0),
        "received": received,
        "processed": processed,
        "success": success,
        "fail": fail,
        "pending_by_metrics": max(0, received - processed),
    }


def print_sample(row: Dict) -> None:
    print(
        f"t={row['t_rel_s']:7.2f}s | "
        f"phase={row['phase']:<24} | "
        f"queue={row['queue_depth']:6} | "
        f"expected={row['expected_workers_now']:2} | "
        f"consumers={row['actual_consumers']:2} | "
        f"processed={row['processed']:7} | "
        f"success={row['success']:7} | "
        f"fail={row['fail']:7}"
    )


def monitor_for(
    system: RemoteMQSystem,
    global_start: float,
    phase: str,
    duration_s: float,
    sample_interval: float,
    rows: List[Dict],
) -> Dict:
    start = time.time()
    phase_rows = []

    while time.time() - start < duration_s:
        row = sample_state(system, global_start, phase)
        rows.append(row)
        phase_rows.append(row)
        print_sample(row)
        time.sleep(sample_interval)

    if not phase_rows:
        return {
            "phase": phase,
            "duration_s": duration_s,
            "max_queue": 0,
            "max_expected_workers": 0,
            "max_actual_consumers": 0,
            "final_queue": 0,
            "final_consumers": 0,
            "processed_delta": 0,
            "success_delta": 0,
            "fail_delta": 0,
        }

    first = phase_rows[0]
    last = phase_rows[-1]

    return {
        "phase": phase,
        "duration_s": duration_s,
        "max_queue": max(r["queue_depth"] for r in phase_rows),
        "max_expected_workers": max(r["expected_workers_now"] for r in phase_rows),
        "max_actual_consumers": max(r["actual_consumers"] for r in phase_rows),
        "final_queue": last["queue_depth"],
        "final_consumers": last["actual_consumers"],
        "processed_delta": last["processed"] - first["processed"],
        "success_delta": last["success"] - first["success"],
        "fail_delta": last["fail"] - first["fail"],
    }


def run_burst_phase(
    system: RemoteMQSystem,
    global_start: float,
    phase_name: str,
    workload: int,
    mode: str,
    monitor_duration: float,
    sample_interval: float,
    rows: List[Dict],
) -> Dict:
    print("\n" + "=" * 72)
    print(f"FASE CARGA: {phase_name} | workload={workload} | mode={mode}")
    print("=" * 72)

    before = sample_state(system, global_start, phase_name + "_before")
    rows.append(before)
    print_sample(before)

    inject_start = time.time()
    publish_s = publish_burst(system, workload, label=phase_name, mode=mode)
    inject_end = time.time()

    print(f"[INJECT] {workload} mensajes publicados en {publish_s:.3f}s ({workload / publish_s:.1f} msg/s)")

    summary = monitor_for(
        system=system,
        global_start=global_start,
        phase=phase_name,
        duration_s=monitor_duration,
        sample_interval=sample_interval,
        rows=rows,
    )

    after = sample_state(system, global_start, phase_name + "_after")
    rows.append(after)

    summary["workload"] = workload
    summary["mode"] = mode
    summary["publish_time_s"] = round(publish_s, 4)
    summary["publish_rate_msg_s"] = round(workload / publish_s if publish_s > 0 else 0, 4)
    summary["expected_initial_workers"] = expected_workers_from_queue(workload)

    return summary


def run_pause_phase(
    system: RemoteMQSystem,
    global_start: float,
    phase_name: str,
    duration_s: float,
    sample_interval: float,
    rows: List[Dict],
) -> Dict:
    print("\n" + "=" * 72)
    print(f"FASE PAUSA: {phase_name} | duration={duration_s}s")
    print("=" * 72)

    summary = monitor_for(
        system=system,
        global_start=global_start,
        phase=phase_name,
        duration_s=duration_s,
        sample_interval=sample_interval,
        rows=rows,
    )

    summary["workload"] = 0
    summary["mode"] = "pause"
    summary["publish_time_s"] = 0
    summary["publish_rate_msg_s"] = 0
    summary["expected_initial_workers"] = MIN_WORKERS

    return summary


def run_benchmark(args) -> None:
    os.makedirs(RESULT_DIR, exist_ok=True)

    system = RemoteMQSystem(args.tower_ip, rest_port=args.rest_port)

    print("=" * 72)
    print("INDIRECT MQ AUTOSCALER BURST BENCHMARK")
    print("=" * 72)
    print(f"TOWER_IP={args.tower_ip}")
    print(f"LB_URL={system.lb_url}")
    print(f"RabbitMQ={args.tower_ip}:{RABBIT_PORT}")
    print(f"MESSAGES_PER_WORKER={MESSAGES_PER_WORKER}")
    print(f"MIN_WORKERS={MIN_WORKERS}, MAX_WORKERS={MAX_WORKERS}")
    print(f"large={args.large}, medium={args.medium}, small={args.small}")
    print("=" * 72)
    print("Recuerda: el autoscaler MQ debe estar arrancado en la torre.")
    print("Este benchmark NO llama a /scale.")

    stamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    samples_csv = os.path.join(RESULT_DIR, f"mq_autoscaler_burst_samples_{stamp}.csv")
    summary_csv = os.path.join(RESULT_DIR, f"mq_autoscaler_burst_summary_{stamp}.csv")
    summary_txt = os.path.join(RESULT_DIR, f"mq_autoscaler_burst_summary_{stamp}.txt")

    if args.reset:
        system.reset()

    initial = sample_state(system, time.time(), "initial_check")
    print("[INITIAL]")
    print_sample(initial)

    if initial["actual_consumers"] == 0:
        print("[WARN] RabbitMQ reporta 0 consumers. Probablemente el autoscaler no está arrancado todavía.")
        print("[WARN] Arranca en la torre: python mq_autoscaler.py")
        if not args.allow_no_consumers:
            print("[ABORT] Usa --allow-no-consumers si quieres continuar igualmente.")
            return

    rows: List[Dict] = []
    summaries: List[Dict] = []
    global_start = time.time()

    # Pausa inicial para observar estado estable.
    summaries.append(
        run_pause_phase(
            system, global_start, "idle_initial", args.initial_pause, args.sample_interval, rows
        )
    )

    # Ráfaga grande -> debería escalar cerca de MAX_WORKERS si large >= 5000.
    summaries.append(
        run_burst_phase(
            system, global_start, "large_burst", args.large, args.mode,
            args.load_monitor, args.sample_interval, rows
        )
    )

    summaries.append(
        run_pause_phase(
            system, global_start, "pause_after_large", args.pause_after_large,
            args.sample_interval, rows
        )
    )

    # Ráfaga mediana -> pico intermedio.
    summaries.append(
        run_burst_phase(
            system, global_start, "medium_burst", args.medium, args.mode,
            args.load_monitor, args.sample_interval, rows
        )
    )

    summaries.append(
        run_pause_phase(
            system, global_start, "pause_after_medium", args.pause_after_medium,
            args.sample_interval, rows
        )
    )

    # Ráfaga pequeña -> debería escalar poco.
    summaries.append(
        run_burst_phase(
            system, global_start, "small_burst", args.small, args.mode,
            args.load_monitor, args.sample_interval, rows
        )
    )

    summaries.append(
        run_pause_phase(
            system, global_start, "pause_after_small", args.pause_after_small,
            args.sample_interval, rows
        )
    )

    sample_fields = [
        "timestamp",
        "t_rel_s",
        "phase",
        "queue_depth",
        "expected_workers_now",
        "actual_consumers",
        "metrics_active_workers",
        "received",
        "processed",
        "success",
        "fail",
        "pending_by_metrics",
    ]

    with open(samples_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=sample_fields)
        writer.writeheader()
        writer.writerows(rows)

    summary_fields = [
        "phase",
        "workload",
        "mode",
        "duration_s",
        "publish_time_s",
        "publish_rate_msg_s",
        "expected_initial_workers",
        "max_queue",
        "max_expected_workers",
        "max_actual_consumers",
        "final_queue",
        "final_consumers",
        "processed_delta",
        "success_delta",
        "fail_delta",
    ]

    with open(summary_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=summary_fields)
        writer.writeheader()
        writer.writerows(summaries)

    with open(summary_txt, "w", encoding="utf-8") as f:
        f.write("INDIRECT MQ AUTOSCALER BURST BENCHMARK\n")
        f.write(f"TOWER_IP={args.tower_ip}\n")
        f.write(f"LB_URL={system.lb_url}\n")
        f.write(f"MESSAGES_PER_WORKER={MESSAGES_PER_WORKER}\n")
        f.write(f"MIN_WORKERS={MIN_WORKERS}, MAX_WORKERS={MAX_WORKERS}\n")
        f.write(f"large={args.large}, medium={args.medium}, small={args.small}\n\n")

        for s in summaries:
            f.write(json.dumps(s, indent=2, ensure_ascii=False))
            f.write("\n\n")

        f.write("=" * 72 + "\n")
        f.write("RESUMEN FINAL\n")
        f.write("=" * 72 + "\n")
        for s in summaries:
            f.write(
                f"{s['phase']} | workload={s['workload']} | "
                f"expected_initial={s['expected_initial_workers']} | "
                f"max_expected={s['max_expected_workers']} | "
                f"max_consumers={s['max_actual_consumers']} | "
                f"final_consumers={s['final_consumers']} | "
                f"final_queue={s['final_queue']} | "
                f"processed_delta={s['processed_delta']}\n"
            )

    print("\n" + "=" * 72)
    print("BENCHMARK COMPLETADO")
    print("=" * 72)
    print(f"Samples CSV: {samples_csv}")
    print(f"Summary CSV: {summary_csv}")
    print(f"Summary TXT: {summary_txt}")

    print("\nInterpretación rápida:")
    print("- large_burst debería provocar un pico alto de consumers, normalmente cerca de 10 si la cola sube bastante.")
    print("- pause_after_large debería enseñar desescalado hasta 1 consumer.")
    print("- medium_burst debería provocar un pico intermedio.")
    print("- small_burst debería provocar un pico bajo.")
    print("- Si actual_consumers no sube, mira la terminal del autoscaler en la torre.")


def parse_args():
    parser = argparse.ArgumentParser(description="Benchmark por ráfagas para autoscaler MQ indirecto.")

    parser.add_argument("--tower-ip", default="100.81.42.52")
    parser.add_argument("--rest-port", type=int, default=8080)

    parser.add_argument("--large", type=int, default=8000, help="Ráfaga grande. 8000 -> expected inicial 10 por cap.")
    parser.add_argument("--medium", type=int, default=2500, help="Ráfaga mediana. 2500 -> expected inicial 5.")
    parser.add_argument("--small", type=int, default=600, help="Ráfaga pequeña. 600 -> expected inicial 2.")
    parser.add_argument("--mode", choices=["unnumbered", "numbered", "hotspot"], default="unnumbered")

    parser.add_argument("--sample-interval", type=float, default=0.5)
    parser.add_argument("--initial-pause", type=float, default=8)
    parser.add_argument("--load-monitor", type=float, default=15)
    parser.add_argument("--pause-after-large", type=float, default=18)
    parser.add_argument("--pause-after-medium", type=float, default=18)
    parser.add_argument("--pause-after-small", type=float, default=18)

    parser.add_argument("--reset", action="store_true", default=True)
    parser.add_argument("--no-reset", dest="reset", action="store_false")
    parser.add_argument("--allow-no-consumers", action="store_true")

    return parser.parse_args()


if __name__ == "__main__":
    run_benchmark(parse_args())

