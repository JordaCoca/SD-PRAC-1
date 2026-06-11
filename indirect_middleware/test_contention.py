import os
import sys
import time
import json
import random
import subprocess
from datetime import datetime

import pika
import requests


# ============================================================
# CONFIGURACIÓN GENERAL
# ============================================================

REST_URL = "http://127.0.0.1:8080"
RESULT_DIR = "resultados"

QUEUE_NAME = "ticket_queue"
RABBIT_HOST = "localhost"
RABBIT_USER = "admin"
RABBIT_PASS = "superpassword"

TOTAL_SEATS = 20000
WORKLOAD = 20000
FIXED_WORKERS = 12

# Estos nombres son IMPORTANTES:
# normal  -> usa mq_worker.py
# hotspot -> usa mq_worker_hotspot.py
WORKER_TYPES = ["normal", "hotspot"]

MODE_LABELS = {
    "normal": "optimistic_setnx",
    "hotspot": "pessimistic_lock"
}

# Parámetros para mq_worker_hotspot.py
CRITICAL_SECTION_SLEEP = "0.005"
LOCK_WAIT_TIMEOUT = "0.3"
LOCK_RETRY_SLEEP = "0.001"

os.makedirs(RESULT_DIR, exist_ok=True)


# ============================================================
# CONEXIONES
# ============================================================

def rabbit_connection():
    credentials = pika.PlainCredentials(RABBIT_USER, RABBIT_PASS)
    parameters = pika.ConnectionParameters(
        host=RABBIT_HOST,
        credentials=credentials
    )
    return pika.BlockingConnection(parameters)


def reset_system():
    print("   [Reset] Limpiando Redis y RabbitMQ...")

    try:
        requests.post(f"{REST_URL}/reset", timeout=10)
    except Exception as e:
        print(f"   [WARN] Error llamando a /reset: {e}")

    try:
        connection = rabbit_connection()
        channel = connection.channel()
        channel.queue_declare(queue=QUEUE_NAME, durable=True)
        channel.queue_purge(queue=QUEUE_NAME)
        connection.close()
    except Exception as e:
        print(f"   [WARN] Error purgando RabbitMQ: {e}")

    time.sleep(2)


def get_metrics():
    try:
        return requests.get(f"{REST_URL}/metrics", timeout=5).json()
    except Exception as e:
        print(f"   [WARN] Error leyendo métricas: {e}")
        return {
            "received": 0,
            "processed": 0,
            "success": 0,
            "fail": 0,
            "active_workers": 0
        }


def get_queue_depth():
    try:
        connection = rabbit_connection()
        channel = connection.channel()
        q = channel.queue_declare(queue=QUEUE_NAME, durable=True)
        count = q.method.message_count
        connection.close()
        return count
    except Exception:
        return -1


# ============================================================
# WORKERS
# ============================================================

def resolve_worker_script(worker_type):
    """
    Busca el worker tanto si ejecutas el benchmark desde la raíz
    como si lo ejecutas desde dentro de mq_app.
    """
    base_path = os.path.dirname(os.path.abspath(__file__))

    if worker_type == "hotspot":
        candidates = [
            os.path.join(base_path, "mq_app", "mq_worker_hotspot.py"),
            os.path.join(base_path, "mq_worker_hotspot.py"),
        ]
    else:
        candidates = [
            os.path.join(base_path, "mq_app", "mq_worker.py"),
            os.path.join(base_path, "mq_worker.py"),
        ]

    for path in candidates:
        if os.path.exists(path):
            return path

    raise FileNotFoundError(
        f"No se ha encontrado el worker para tipo={worker_type}. "
        f"Buscado en: {candidates}"
    )


def start_workers(num_workers, worker_type):
    label = MODE_LABELS.get(worker_type, worker_type)
    print(f"   [Workers] Iniciando {num_workers} workers tipo {label}...")

    procesos = []
    worker_script = resolve_worker_script(worker_type)
    python_exe = sys.executable

    print(f"   [Workers] Script usado: {worker_script}")

    for i in range(num_workers):
        env = os.environ.copy()

        env["WORKER_ID"] = f"mq-{worker_type}-{i + 1}"

        # Solo mq_worker_hotspot.py usa estos valores, pero no molesta pasarlos siempre.
        env["CRITICAL_SECTION_SLEEP"] = CRITICAL_SECTION_SLEEP
        env["LOCK_WAIT_TIMEOUT"] = LOCK_WAIT_TIMEOUT
        env["LOCK_RETRY_SLEEP"] = LOCK_RETRY_SLEEP

        env["RABBIT_HOST"] = RABBIT_HOST
        env["RABBIT_USER"] = RABBIT_USER
        env["RABBIT_PASS"] = RABBIT_PASS
        env["QUEUE_NAME"] = QUEUE_NAME

        p = subprocess.Popen(
            [python_exe, worker_script],
            env=env
        )

        procesos.append(p)

    time.sleep(2)
    return procesos


def stop_workers(procesos):
    print("   [Workers] Deteniendo workers...")

    for p in procesos:
        try:
            p.terminate()
        except Exception:
            pass

    for p in procesos:
        try:
            p.wait(timeout=5)
        except subprocess.TimeoutExpired:
            p.kill()


# ============================================================
# GENERADORES DE ASIENTOS
# ============================================================

def seat_normal(i):
    """
    Caso normal:
    reparte las peticiones entre los 20.000 asientos.
    """
    return (i % TOTAL_SEATS) + 1


def seat_uniform_limited(i, num_seats):
    """
    Contención uniforme sobre pocos asientos.
    """
    return (i % num_seats) + 1


def seat_hotspot_80_5():
    """
    Hotspot del enunciado:
    80% de peticiones van al 5% de los asientos.

    5% de 20.000 = 1.000 asientos calientes.
    """
    hot_seats = int(TOTAL_SEATS * 0.05)  # 1000

    if random.random() < 0.80:
        return random.randint(1, hot_seats)
    else:
        return random.randint(hot_seats + 1, TOTAL_SEATS)


# ============================================================
# INYECCIÓN DE WORKLOAD
# ============================================================

def inject_workload(scenario_name):
    connection = rabbit_connection()
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)

    print(f"   [Inject] Enviando {WORKLOAD} peticiones: {scenario_name}")

    start = time.time()

    for i in range(WORKLOAD):
        if scenario_name == "normal_20000":
            seat_id = seat_normal(i)

        elif scenario_name == "hotspot_80_5":
            seat_id = seat_hotspot_80_5()

        elif scenario_name == "limited_2000":
            seat_id = seat_uniform_limited(i, 2000)

        elif scenario_name == "limited_200":
            seat_id = seat_uniform_limited(i, 200)

        elif scenario_name == "limited_20":
            seat_id = seat_uniform_limited(i, 20)

        else:
            raise ValueError(f"Escenario desconocido: {scenario_name}")

        message = {
            "client_id": f"c_{i}",
            "seat_id": seat_id,
            "request_id": f"req_{scenario_name}_{i}"
        }

        channel.basic_publish(
            exchange="",
            routing_key=QUEUE_NAME,
            body=json.dumps(message),
            properties=pika.BasicProperties(delivery_mode=2)
        )

    connection.close()

    elapsed = time.time() - start
    print(f"   [Inject] Envío terminado en {elapsed:.2f}s")


# ============================================================
# MEDICIÓN
# ============================================================

def wait_until_processed(expected_messages, timeout=180):
    start = time.time()

    while True:
        metrics = get_metrics()
        processed = metrics.get("processed", 0)
        success = metrics.get("success", 0)
        fail = metrics.get("fail", 0)
        queue_depth = get_queue_depth()

        print(
            f"   [Wait] processed={processed}/{expected_messages} | "
            f"success={success} | "
            f"fail={fail} | "
            f"queue={queue_depth}"
        )

        if processed >= expected_messages:
            return time.time() - start, metrics

        if time.time() - start > timeout:
            print("   [WARN] Timeout esperando procesamiento.")
            return time.time() - start, metrics

        time.sleep(0.5)


def validate_numbered_result(metrics):
    success = metrics.get("success", 0)
    fail = metrics.get("fail", 0)
    processed = metrics.get("processed", 0)

    ok = True
    notes = []

    if success > TOTAL_SEATS:
        ok = False
        notes.append("ERROR: success > 20000, posible overselling.")

    if success + fail < processed:
        notes.append("WARN: success + fail menor que processed, posible desfase de métricas.")

    if not notes:
        notes.append("OK")

    return ok, " ".join(notes)


# ============================================================
# BENCHMARK
# ============================================================

def run_single_case(worker_type, scenario_name):
    reset_system()

    workers = start_workers(FIXED_WORKERS, worker_type)

    try:
        start_total = time.time()

        inject_workload(scenario_name)

        _, metrics = wait_until_processed(WORKLOAD)

        total_duration = time.time() - start_total
        throughput = WORKLOAD / total_duration if total_duration > 0 else 0

        ok, validation_msg = validate_numbered_result(metrics)

        return {
            "worker_type": worker_type,
            "mode_label": MODE_LABELS.get(worker_type, worker_type),
            "scenario": scenario_name,
            "workers": FIXED_WORKERS,
            "workload": WORKLOAD,
            "total_time_s": total_duration,
            "throughput_msg_s": throughput,
            "success": metrics.get("success", 0),
            "fail": metrics.get("fail", 0),
            "processed": metrics.get("processed", 0),
            "valid": ok,
            "validation": validation_msg
        }

    finally:
        stop_workers(workers)
        time.sleep(2)


def run_benchmark():
    fecha = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    file_path = os.path.join(RESULT_DIR, f"contention_benchmark_{fecha}.txt")
    csv_path = os.path.join(RESULT_DIR, f"contention_benchmark_{fecha}.csv")

    scenarios = [
        "normal_20000",
        "hotspot_80_5",
        "limited_2000",
        "limited_200",
        "limited_20",
    ]

    all_results = []

    print("================================================")
    print("BENCHMARK DE CONTENCIÓN RABBITMQ + REDIS")
    print("================================================")
    print(f"Workload: {WORKLOAD}")
    print(f"Workers fijos: {FIXED_WORKERS}")
    print(f"Worker types: {WORKER_TYPES}")
    print(f"Critical section sleep: {CRITICAL_SECTION_SLEEP}")
    print(f"Lock wait timeout: {LOCK_WAIT_TIMEOUT}")
    print(f"Lock retry sleep: {LOCK_RETRY_SLEEP}")
    print(f"Resultados TXT: {file_path}")
    print(f"Resultados CSV: {csv_path}")
    print("================================================")

    with open(file_path, "w", encoding="utf-8") as f:
        f.write("--- BENCHMARK DE CONTENCION RABBITMQ + REDIS ---\n")
        f.write(f"Fecha: {fecha}\n")
        f.write(f"Workload: {WORKLOAD}\n")
        f.write(f"Workers fijos: {FIXED_WORKERS}\n")
        f.write(f"Worker types: {WORKER_TYPES}\n")
        f.write(f"Critical section sleep: {CRITICAL_SECTION_SLEEP}\n")
        f.write(f"Lock wait timeout: {LOCK_WAIT_TIMEOUT}\n")
        f.write(f"Lock retry sleep: {LOCK_RETRY_SLEEP}\n\n")

        header = (
            f"{'Mode':18} | {'Scenario':15} | {'Time(s)':>10} | "
            f"{'Throughput':>12} | {'Success':>7} | {'Fail':>7} | "
            f"{'Processed':>9} | {'Valid':>5} | Validation\n"
        )

        f.write(header)
        f.write("-" * 125 + "\n")

        print(header.strip())
        print("-" * 125)

        for worker_type in WORKER_TYPES:
            for scenario in scenarios:
                print(f"\n>>> Ejecutando mode={MODE_LABELS[worker_type]}, scenario={scenario}")

                result = run_single_case(worker_type, scenario)
                all_results.append(result)

                line = (
                    f"{result['mode_label']:18} | "
                    f"{result['scenario']:15} | "
                    f"{result['total_time_s']:10.2f} | "
                    f"{result['throughput_msg_s']:12.2f} | "
                    f"{result['success']:7} | "
                    f"{result['fail']:7} | "
                    f"{result['processed']:9} | "
                    f"{str(result['valid']):>5} | "
                    f"{result['validation']}\n"
                )

                f.write(line)
                f.flush()

                print(line.strip())

    with open(csv_path, "w", encoding="utf-8") as f:
        f.write(
            "mode,worker_type,scenario,workers,workload,total_time_s,"
            "throughput_msg_s,success,fail,processed,valid,validation\n"
        )

        for r in all_results:
            f.write(
                f"{r['mode_label']},"
                f"{r['worker_type']},"
                f"{r['scenario']},"
                f"{r['workers']},"
                f"{r['workload']},"
                f"{r['total_time_s']:.4f},"
                f"{r['throughput_msg_s']:.4f},"
                f"{r['success']},"
                f"{r['fail']},"
                f"{r['processed']},"
                f"{r['valid']},"
                f"\"{r['validation']}\"\n"
            )

    print("\n================================================")
    print("BENCHMARK COMPLETADO")
    print("================================================")
    print(f"TXT: {file_path}")
    print(f"CSV: {csv_path}")
    print("================================================")


if __name__ == "__main__":
    run_benchmark()