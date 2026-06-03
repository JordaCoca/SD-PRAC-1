import os
import csv
import json
import time
import math
import requests
import pika
from datetime import datetime

# ============================================================
# CONFIGURACIÓN
# ============================================================

REST_URL = "http://127.0.0.1:8080"

RABBIT_HOST = "localhost"
RABBIT_PORT = 5672
RABBIT_USER = "admin"
RABBIT_PASS = "superpassword"

QUEUE_NAME = "ticket_queue"
RESULT_DIR = "resultados"

# Debe coincidir con mq_autoscaler.py
MESSAGES_PER_WORKER = 500
MIN_WORKERS = 1
MAX_WORKERS = 10

# Cada cuántos segundos se muestrea el estado
SAMPLE_INTERVAL = 0.5

# Tiempo máximo para cada fase
PHASE_TIMEOUT = 45

os.makedirs(RESULT_DIR, exist_ok=True)


# ============================================================
# CONEXIONES
# ============================================================

def rabbit_connection():
    credentials = pika.PlainCredentials(RABBIT_USER, RABBIT_PASS)
    params = pika.ConnectionParameters(
        host=RABBIT_HOST,
        port=RABBIT_PORT,
        virtual_host="/",
        credentials=credentials
    )
    return pika.BlockingConnection(params)


def reset_system():
    print("[RESET] Limpiando Redis + RabbitMQ...")
    try:
        requests.post(f"{REST_URL}/reset", timeout=5)
    except Exception as e:
        print(f"[WARN] No se pudo llamar a /reset: {e}")

    try:
        connection = rabbit_connection()
        channel = connection.channel()
        channel.queue_declare(queue=QUEUE_NAME, durable=True)
        channel.queue_purge(queue=QUEUE_NAME)
        connection.close()
    except Exception as e:
        print(f"[WARN] No se pudo purgar RabbitMQ: {e}")

    time.sleep(2)


def get_queue_state():
    """
    Devuelve:
    - mensajes pendientes en cola
    - consumidores conectados a la cola

    consumer_count es muy útil aquí porque cada worker MQ consume de ticket_queue.
    Así medimos workers reales, no los contadores Redis antiguos.
    """
    connection = rabbit_connection()
    channel = connection.channel()
    q = channel.queue_declare(queue=QUEUE_NAME, durable=True, passive=False)

    pending = q.method.message_count
    consumers = q.method.consumer_count

    connection.close()
    return pending, consumers


def get_metrics():
    try:
        return requests.get(f"{REST_URL}/metrics", timeout=3).json()
    except Exception:
        return {
            "received": 0,
            "processed": 0,
            "success": 0,
            "fail": 0,
            "active_workers": 0
        }


# ============================================================
# INYECCIÓN DE WORKLOAD
# ============================================================

def publish_burst(total_messages, mode="unnumbered"):
    """
    Publica todos los mensajes lo más rápido posible.
    Esto fuerza backlog para que el autoscaler tenga algo que detectar.
    """
    connection = rabbit_connection()
    channel = connection.channel()
    channel.queue_declare(queue=QUEUE_NAME, durable=True)

    for i in range(total_messages):
        if mode == "unnumbered":
            message = {
                "client_id": f"autoscale_client_{i}",
                "seat_id": None,
                "request_id": f"autoscale_req_{int(time.time())}_{i}"
            }
        elif mode == "numbered":
            seat_id = (i % 20000) + 1
            message = {
                "client_id": f"autoscale_client_{i}",
                "seat_id": seat_id,
                "request_id": f"autoscale_req_{int(time.time())}_{i}"
            }
        elif mode == "hotspot":
            # 80% de peticiones al 5% de asientos: 1000 asientos calientes
            if i % 10 < 8:
                seat_id = (i % 1000) + 1
            else:
                seat_id = 1001 + (i % 19000)

            message = {
                "client_id": f"autoscale_client_{i}",
                "seat_id": seat_id,
                "request_id": f"autoscale_req_{int(time.time())}_{i}"
            }
        else:
            raise ValueError(f"Modo desconocido: {mode}")

        channel.basic_publish(
            exchange="",
            routing_key=QUEUE_NAME,
            body=json.dumps(message),
            properties=pika.BasicProperties(delivery_mode=2)
        )

    connection.close()


# ============================================================
# LÓGICA DE VALIDACIÓN
# ============================================================

def expected_workers_from_queue(queue_depth):
    desired = math.ceil(queue_depth / MESSAGES_PER_WORKER)
    desired = max(MIN_WORKERS, min(desired, MAX_WORKERS))
    return desired


def sample_state(phase_name, start_time):
    pending, consumers = get_queue_state()
    metrics = get_metrics()
    expected = expected_workers_from_queue(pending)

    return {
        "timestamp": datetime.now().isoformat(timespec="seconds"),
        "t_rel_s": round(time.time() - start_time, 2),
        "phase": phase_name,
        "queue_depth": pending,
        "expected_workers_now": expected,
        "actual_consumers": consumers,
        "processed": metrics.get("processed", 0),
        "success": metrics.get("success", 0),
        "fail": metrics.get("fail", 0),
        # Ojo: este campo puede quedar contaminado por métricas antiguas de workers parados.
        # Lo dejamos como referencia, pero la métrica buena para autoscaling es actual_consumers.
        "metrics_active_workers": metrics.get("active_workers", 0),
    }


def run_phase(phase_name, workload, mode, csv_writer, global_start):
    print(f"\n=== FASE: {phase_name} | workload={workload} | mode={mode} ===")

    before_pending, before_consumers = get_queue_state()
    before_metrics = get_metrics()
    before_processed = before_metrics.get("processed", 0)

    print(f"[ANTES] cola={before_pending}, consumers={before_consumers}, processed={before_processed}")

    if workload > 0:
        t_inject_start = time.time()
        publish_burst(workload, mode=mode)
        t_inject_end = time.time()
        print(f"[INJECT] {workload} mensajes enviados en {t_inject_end - t_inject_start:.2f}s")

    phase_start = time.time()
    max_consumers_seen = 0
    max_queue_seen = 0
    max_expected_seen = 0

    reached_expected = False
    drained = False

    while True:
        row = sample_state(phase_name, global_start)
        csv_writer.writerow(row)

        q = row["queue_depth"]
        consumers = row["actual_consumers"]
        expected = row["expected_workers_now"]

        max_consumers_seen = max(max_consumers_seen, consumers)
        max_queue_seen = max(max_queue_seen, q)
        max_expected_seen = max(max_expected_seen, expected)

        print(
            f"t={row['t_rel_s']:6.1f}s | "
            f"cola={q:6} | "
            f"workers_reales={consumers:2} | "
            f"workers_esperados_ahora={expected:2} | "
            f"processed={row['processed']:6} | "
            f"success={row['success']:6} | "
            f"fail={row['fail']:6}"
        )

        # Consideramos que ha escalado bien si en algún momento alcanza
        # al menos el número esperado máximo observado menos 1.
        # Le damos margen porque la cola puede drenarse mientras escala.
        if max_expected_seen <= 1:
            reached_expected = True
        elif consumers >= max_expected_seen - 1:
            reached_expected = True

        # Consideramos fase drenada cuando ya no queda cola.
        if q == 0:
            drained = True

        elapsed = time.time() - phase_start

        # Para cargas pequeñas, cuando drena podemos pasar.
        # Para cargas grandes, esperamos además haber visto escalado.
        if drained and reached_expected and elapsed >= 3:
            break

        if elapsed > PHASE_TIMEOUT:
            print("[WARN] Timeout de fase alcanzado.")
            break

        time.sleep(SAMPLE_INTERVAL)

    after_pending, after_consumers = get_queue_state()
    after_metrics = get_metrics()
    after_processed = after_metrics.get("processed", 0)

    processed_in_phase = after_processed - before_processed

    print(
        f"[RESUMEN FASE] max_cola={max_queue_seen}, "
        f"max_workers_esperados={max_expected_seen}, "
        f"max_workers_reales={max_consumers_seen}, "
        f"processed_fase={processed_in_phase}, "
        f"cola_final={after_pending}, "
        f"workers_finales={after_consumers}"
    )

    return {
        "phase": phase_name,
        "workload": workload,
        "mode": mode,
        "max_queue_seen": max_queue_seen,
        "max_expected_workers_seen": max_expected_seen,
        "max_actual_workers_seen": max_consumers_seen,
        "processed_in_phase": processed_in_phase,
        "final_queue": after_pending,
        "final_consumers": after_consumers,
        "reached_expected": reached_expected,
        "drained": drained,
    }


def wait_for_scale_down(csv_writer, global_start, target_workers=1, timeout=30):
    print(f"\n=== FASE: ESPERANDO SCALE DOWN A {target_workers} WORKER(S) ===")

    start = time.time()
    reached = False

    while time.time() - start < timeout:
        row = sample_state("scale_down_wait", global_start)
        csv_writer.writerow(row)

        print(
            f"t={row['t_rel_s']:6.1f}s | "
            f"cola={row['queue_depth']:6} | "
            f"workers_reales={row['actual_consumers']:2}"
        )

        if row["queue_depth"] == 0 and row["actual_consumers"] <= target_workers:
            reached = True
            break

        time.sleep(SAMPLE_INTERVAL)

    print(f"[SCALE DOWN] reached={reached}")
    return reached


# ============================================================
# MAIN
# ============================================================

def run_autoscaler_benchmark():
    fecha = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")

    samples_path = os.path.join(RESULT_DIR, f"autoscaler_samples_{fecha}.csv")
    summary_path = os.path.join(RESULT_DIR, f"autoscaler_summary_{fecha}.csv")

    reset_system()

    # Fases diseñadas según MESSAGES_PER_WORKER = 500
    phases = [
        # nombre, workload, modo
        ("idle_initial", 0, "unnumbered"),
        ("small_load_expect_1_worker", 200, "unnumbered"),
        ("medium_load_expect_2_workers", 600, "unnumbered"),
        ("medium_load_expect_3_workers", 1200, "unnumbered"),
        ("high_load_expect_5_workers", 2500, "unnumbered"),
        ("very_high_load_expect_9_workers", 4500, "unnumbered"),
        ("cap_test_expect_10_workers", 8000, "unnumbered"),
    ]

    global_start = time.time()
    summaries = []

    fieldnames = [
        "timestamp",
        "t_rel_s",
        "phase",
        "queue_depth",
        "expected_workers_now",
        "actual_consumers",
        "processed",
        "success",
        "fail",
        "metrics_active_workers",
    ]

    with open(samples_path, "w", newline="", encoding="utf-8") as f_samples:
        writer = csv.DictWriter(f_samples, fieldnames=fieldnames)
        writer.writeheader()

        for phase_name, workload, mode in phases:
            summary = run_phase(
                phase_name=phase_name,
                workload=workload,
                mode=mode,
                csv_writer=writer,
                global_start=global_start
            )
            summaries.append(summary)

            # Pequeña pausa entre fases para ver estabilidad.
            time.sleep(2)

        scale_down_ok = wait_for_scale_down(
            csv_writer=writer,
            global_start=global_start,
            target_workers=1,
            timeout=30
        )

    with open(summary_path, "w", newline="", encoding="utf-8") as f_summary:
        fieldnames_summary = [
            "phase",
            "workload",
            "mode",
            "max_queue_seen",
            "max_expected_workers_seen",
            "max_actual_workers_seen",
            "processed_in_phase",
            "final_queue",
            "final_consumers",
            "reached_expected",
            "drained",
        ]

        writer = csv.DictWriter(f_summary, fieldnames=fieldnames_summary)
        writer.writeheader()
        for s in summaries:
            writer.writerow(s)

    print("\n================================================")
    print("BENCHMARK AUTOSCALER COMPLETADO")
    print("================================================")
    print(f"Samples CSV: {samples_path}")
    print(f"Summary CSV: {summary_path}")
    print(f"Scale down final a 1 worker: {scale_down_ok}")

    print("\nInterpretación rápida:")
    print("- max_expected_workers_seen: workers que el autoscaler debería haber querido según la cola.")
    print("- max_actual_workers_seen: workers/consumers reales vistos en RabbitMQ.")
    print("- Si max_actual_workers_seen se acerca a max_expected_workers_seen, el escalado funciona.")
    print("- Si final_consumers vuelve a 1 tras drenar la cola, el desescalado funciona.")


if __name__ == "__main__":
    run_autoscaler_benchmark()