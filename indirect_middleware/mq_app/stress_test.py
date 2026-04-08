import pika
import json
import time


def run_stress_test_unnumbered(total_requests=3000):
    # Conexión a RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # Aseguramos que la cola existe
    channel.queue_declare(queue='ticket_queue', durable=True)

    print(f" Lanzando experimento: {total_requests} peticiones UNNUMBERED...")

    start_time = time.time()

    for i in range(total_requests):
        # Creamos el mensaje SIN seat_id para forzar lógica Unnumbered
        message = {
            "client_id": f"client_{i}",
            "seat_id": None,
            "request_id": f"req_{i}"
        }

        channel.basic_publish(
            exchange='',
            routing_key='ticket_queue',
            body=json.dumps(message),
            properties=pika.BasicProperties(
                delivery_mode=2,  # Mensaje persistente
            )
        )

        if (i + 1) % 500 == 0:
            print(f"  > {i + 1} mensajes enviados...")

    connection.close()
    end_time = time.time()

    duration = end_time - start_time
    print("-" * 30)
    print(f" ENVÍO COMPLETADO")
    print(f" Tiempo de inyección: {duration:.2f} segundos")
    print(f" Tasa de envío: {total_requests / duration:.2f} msg/s")
    print("-" * 30)


def run_numbered_test(total_requests=1000, num_seats=100):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='ticket_queue', durable=True)

    print(f" Enviando {total_requests} peticiones para {num_seats} asientos distintos...")

    for i in range(total_requests):
        # El asiento será: 1, 2, 3... 100, 1, 2, 3...
        seat_id = (i % num_seats) + 1

        message = {
            "client_id": f"client_{i}",
            "seat_id": seat_id,  # Enviamos el ID numérico
            "request_id": f"req_{i}"
        }

        channel.basic_publish(
            exchange='',
            routing_key='ticket_queue',
            body=json.dumps(message),
            properties=pika.BasicProperties(delivery_mode=2)
        )

    connection.close()
    print(" Inyección completada.")


if __name__ == "__main__":
    #run_stress_test_unnumbered(3000)
    run_numbered_test()