#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
send_buy_benchmark.py

Parser y emisor de benchmarks en formato textual del enunciado:

Numbered:
    BUY <client_id> <seat_id> <request_id>
    BUY user00001 42 00001

Unnumbered:
    BUY <client_id> <request_id>
    BUY user00001 00001

Convierte cada línea BUY a JSON equivalente y lo envía a:
  - arquitectura directa REST: POST /buy
  - arquitectura indirecta RabbitMQ: publish en ticket_queue

Representación JSON usada:

Numbered:
    {"client_id": "user00001", "seat_id": 42, "request_id": "00001"}

Unnumbered:
    {"client_id": "user00001", "seat_id": null, "request_id": "00001"}

Uso directo REST:
    python send_buy_benchmark.py \
      --input benchmark_numbered.txt \
      --mode direct \
      --type numbered \
      --url http://100.81.42.52:8080

Uso indirecto RabbitMQ:
    python send_buy_benchmark.py \
      --input benchmark_numbered.txt \
      --mode indirect \
      --type numbered \
      --rabbit-host 100.81.42.52

Para unnumbered:
    python send_buy_benchmark.py --input benchmark_unnumbered.txt --mode indirect --type unnumbered --rabbit-host 100.81.42.52

Notas:
  - Ignora líneas vacías.
  - Ignora comentarios que empiezan por #.
  - Valida que numbered tenga 4 tokens y unnumbered tenga 3 tokens.
  - Valida que el primer token sea BUY.
  - En numbered valida seat_id entre 1 y 20000 por defecto.
"""

import argparse
import json
import sys
import time
from dataclasses import dataclass
from typing import Dict, Iterable, List, Optional, Tuple

import requests
import pika


QUEUE_NAME = "ticket_queue"
DEFAULT_RABBIT_USER = "admin"
DEFAULT_RABBIT_PASS = "superpassword"


@dataclass
class ParsedRequest:
    client_id: str
    request_id: str
    seat_id: Optional[int]

    def to_json(self) -> Dict:
        return {
            "client_id": self.client_id,
            "seat_id": self.seat_id,
            "request_id": self.request_id,
        }


def parse_buy_line(line: str, line_no: int, bench_type: str, max_seat: int) -> Optional[ParsedRequest]:
    """
    Devuelve None para líneas vacías o comentarios.
    Lanza ValueError para líneas inválidas.
    """
    original = line.rstrip("\n")
    line = line.strip()

    if not line:
        return None

    if line.startswith("#"):
        return None

    parts = line.split()

    if bench_type == "numbered":
        expected_tokens = 4
        if len(parts) != expected_tokens:
            raise ValueError(
                f"Line {line_no}: numbered espera formato "
                f"'BUY <client_id> <seat_id> <request_id>', pero hay {len(parts)} tokens: {original}"
            )

        op, client_id, seat_raw, request_id = parts

        if op != "BUY":
            raise ValueError(f"Line {line_no}: operación inválida '{op}', se esperaba BUY")

        try:
            seat_id = int(seat_raw)
        except ValueError:
            raise ValueError(f"Line {line_no}: seat_id no es entero: {seat_raw}")

        if seat_id < 1 or seat_id > max_seat:
            raise ValueError(f"Line {line_no}: seat_id fuera de rango 1..{max_seat}: {seat_id}")

        return ParsedRequest(client_id=client_id, seat_id=seat_id, request_id=request_id)

    if bench_type == "unnumbered":
        expected_tokens = 3
        if len(parts) != expected_tokens:
            raise ValueError(
                f"Line {line_no}: unnumbered espera formato "
                f"'BUY <client_id> <request_id>', pero hay {len(parts)} tokens: {original}"
            )

        op, client_id, request_id = parts

        if op != "BUY":
            raise ValueError(f"Line {line_no}: operación inválida '{op}', se esperaba BUY")

        return ParsedRequest(client_id=client_id, seat_id=None, request_id=request_id)

    raise ValueError(f"Tipo desconocido: {bench_type}")


def load_requests(path: str, bench_type: str, max_seat: int) -> List[ParsedRequest]:
    requests_list: List[ParsedRequest] = []

    with open(path, "r", encoding="utf-8") as f:
        for line_no, line in enumerate(f, start=1):
            parsed = parse_buy_line(line, line_no, bench_type, max_seat)
            if parsed is not None:
                requests_list.append(parsed)

    return requests_list


def send_direct(
    requests_list: List[ParsedRequest],
    base_url: str,
    batch_log: int,
    timeout: float,
) -> Tuple[int, int, int, float]:
    """
    Envía peticiones al endpoint REST /buy.
    Devuelve: success_count, fail_count, http_error_count, elapsed_s.

    Asume que el worker/load balancer acepta JSON:
      {"client_id": ..., "seat_id": ..., "request_id": ...}
    """
    url = base_url.rstrip("/") + "/buy"

    success_count = 0
    fail_count = 0
    http_error_count = 0

    start = time.time()

    session = requests.Session()

    for i, req in enumerate(requests_list, start=1):
        try:
            r = session.post(url, json=req.to_json(), timeout=timeout)

            if r.status_code >= 500:
                http_error_count += 1
            else:
                # Intentamos interpretar la respuesta.
                # Si no sabemos el formato exacto, usamos heurística.
                try:
                    data = r.json()
                except Exception:
                    data = {}

                raw = json.dumps(data).lower()

                if r.status_code == 200 and (
                    data.get("status") == "success"
                    or data.get("result") == "success"
                    or data.get("success") is True
                    or "success" in raw
                    or "ok" in raw
                ):
                    success_count += 1
                elif r.status_code == 200 and (
                    data.get("status") == "fail"
                    or data.get("result") == "fail"
                    or data.get("success") is False
                    or "fail" in raw
                    or "sold" in raw
                    or "unavailable" in raw
                    or "no tickets" in raw
                ):
                    fail_count += 1
                elif r.status_code in (400, 404, 409):
                    fail_count += 1
                else:
                    # Si no queda claro, lo contamos como http_error para no falsear resultados.
                    http_error_count += 1

        except Exception as e:
            http_error_count += 1
            print(f"[WARN] request {i} failed: {e}")

        if batch_log and i % batch_log == 0:
            print(f"  sent {i}/{len(requests_list)}")

    elapsed = time.time() - start
    return success_count, fail_count, http_error_count, elapsed


def send_indirect(
    requests_list: List[ParsedRequest],
    rabbit_host: str,
    rabbit_port: int,
    rabbit_user: str,
    rabbit_pass: str,
    queue_name: str,
    batch_log: int,
) -> Tuple[int, float]:
    """
    Publica mensajes en RabbitMQ.
    Devuelve: published_count, elapsed_s.

    El resultado success/fail se consulta luego con /metrics del load balancer.
    """
    credentials = pika.PlainCredentials(rabbit_user, rabbit_pass)
    params = pika.ConnectionParameters(
        host=rabbit_host,
        port=rabbit_port,
        virtual_host="/",
        credentials=credentials,
        heartbeat=600,
        blocked_connection_timeout=300,
    )

    conn = pika.BlockingConnection(params)
    ch = conn.channel()
    ch.queue_declare(queue=queue_name, durable=True)

    props = pika.BasicProperties(delivery_mode=2)

    start = time.time()

    for i, req in enumerate(requests_list, start=1):
        ch.basic_publish(
            exchange="",
            routing_key=queue_name,
            body=json.dumps(req.to_json()),
            properties=props,
        )

        if batch_log and i % batch_log == 0:
            print(f"  published {i}/{len(requests_list)}")

    conn.close()
    elapsed = time.time() - start
    return len(requests_list), elapsed


def wait_metrics(base_url: str, expected_processed: int, timeout_s: float, interval_s: float) -> Dict:
    """
    Espera a que /metrics tenga processed >= expected_processed.
    """
    metrics_url = base_url.rstrip("/") + "/metrics"
    start = time.time()
    last = {}

    while True:
        try:
            last = requests.get(metrics_url, timeout=5).json()
        except Exception as e:
            print(f"[WARN] metrics failed: {e}")
            last = {}

        processed = int(last.get("processed", 0))
        success = int(last.get("success", 0))
        fail = int(last.get("fail", 0))
        active = int(last.get("active_workers", 0))

        print(
            f"  [WAIT] processed={processed}/{expected_processed} "
            f"success={success} fail={fail} active_workers={active}"
        )

        if processed >= expected_processed:
            return last

        if time.time() - start > timeout_s:
            print("[WARN] Timeout esperando métricas")
            return last

        time.sleep(interval_s)


def maybe_reset(base_url: Optional[str]) -> None:
    if not base_url:
        return

    url = base_url.rstrip("/") + "/reset"
    print(f"[RESET] POST {url}")

    r = requests.post(url, timeout=30)
    print(f"[RESET] status={r.status_code} body={r.text[:200]}")
    r.raise_for_status()

    time.sleep(1.0)


def maybe_scale(base_url: Optional[str], workers: Optional[int]) -> None:
    if not base_url or workers is None:
        return

    url = base_url.rstrip("/") + f"/scale?num_workers={workers}"
    print(f"[SCALE] POST {url}")

    r = requests.post(url, timeout=30)
    print(f"[SCALE] status={r.status_code} body={r.text[:200]}")
    r.raise_for_status()

    time.sleep(2.0)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Parser/emisor de benchmarks BUY en formato textual del enunciado."
    )

    parser.add_argument("--input", required=True, help="Fichero .txt con líneas BUY")
    parser.add_argument("--type", choices=["numbered", "unnumbered"], required=True)
    parser.add_argument("--mode", choices=["direct", "indirect"], required=True)

    parser.add_argument("--url", default="http://100.81.42.52:8080", help="URL base del LB REST")
    parser.add_argument("--rabbit-host", default="100.81.42.52")
    parser.add_argument("--rabbit-port", type=int, default=5672)
    parser.add_argument("--rabbit-user", default=DEFAULT_RABBIT_USER)
    parser.add_argument("--rabbit-pass", default=DEFAULT_RABBIT_PASS)
    parser.add_argument("--queue", default=QUEUE_NAME)

    parser.add_argument("--max-seat", type=int, default=20000)
    parser.add_argument("--batch-log", type=int, default=5000)

    parser.add_argument("--reset", action="store_true", help="Llama a /reset antes de enviar")
    parser.add_argument("--scale", type=int, default=None, help="Llama a /scale?num_workers=N antes de enviar")

    parser.add_argument("--wait", action="store_true", help="Tras enviar en indirecto, espera a /metrics processed=N")
    parser.add_argument("--wait-timeout", type=float, default=300.0)
    parser.add_argument("--wait-interval", type=float, default=0.5)

    parser.add_argument("--http-timeout", type=float, default=10.0)

    return parser.parse_args()


def main():
    args = parse_args()

    requests_list = load_requests(args.input, args.type, args.max_seat)

    print("=" * 72)
    print("BUY BENCHMARK SENDER")
    print("=" * 72)
    print(f"input={args.input}")
    print(f"type={args.type}")
    print(f"mode={args.mode}")
    print(f"requests={len(requests_list)}")
    print("=" * 72)

    if not requests_list:
        print("[ERROR] No hay peticiones válidas en el fichero.")
        sys.exit(1)

    if args.reset:
        maybe_reset(args.url)

    if args.scale is not None:
        maybe_scale(args.url, args.scale)

    if args.mode == "direct":
        success, fail, http_error, elapsed = send_direct(
            requests_list=requests_list,
            base_url=args.url,
            batch_log=args.batch_log,
            timeout=args.http_timeout,
        )

        print("\n" + "=" * 72)
        print("RESULT DIRECT")
        print("=" * 72)
        print(f"sent={len(requests_list)}")
        print(f"success={success}")
        print(f"fail={fail}")
        print(f"http_error={http_error}")
        print(f"elapsed_s={elapsed:.4f}")
        print(f"throughput_req_s={len(requests_list) / elapsed if elapsed > 0 else 0:.4f}")

    elif args.mode == "indirect":
        published, elapsed = send_indirect(
            requests_list=requests_list,
            rabbit_host=args.rabbit_host,
            rabbit_port=args.rabbit_port,
            rabbit_user=args.rabbit_user,
            rabbit_pass=args.rabbit_pass,
            queue_name=args.queue,
            batch_log=args.batch_log,
        )

        print("\n" + "=" * 72)
        print("RESULT INDIRECT PUBLISH")
        print("=" * 72)
        print(f"published={published}")
        print(f"elapsed_s={elapsed:.4f}")
        print(f"publish_rate_msg_s={published / elapsed if elapsed > 0 else 0:.4f}")

        if args.wait:
            print("\n" + "=" * 72)
            print("WAITING METRICS")
            print("=" * 72)

            metrics = wait_metrics(
                base_url=args.url,
                expected_processed=published,
                timeout_s=args.wait_timeout,
                interval_s=args.wait_interval,
            )

            print("\n" + "=" * 72)
            print("FINAL METRICS")
            print("=" * 72)
            print(json.dumps(metrics, indent=2))

    else:
        raise ValueError(args.mode)


if __name__ == "__main__":
    main()
