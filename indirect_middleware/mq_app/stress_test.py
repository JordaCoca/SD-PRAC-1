import pika
import json
import time

def send_test_batch(n=2000):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='ticket_queue', durable=True)

    print(f" Enviando {n} peticiones a la cola...")
    for i in range(n):
        message = {
            "client_id": f"user_{i}",
            "seat_id": i if i % 2 == 0 else None, # Mezcla numeradas y no numeradas
            "request_id": f"req_{i}"
        }
        channel.basic_publish(
            exchange='',
            routing_key='ticket_queue',
            body=json.dumps(message),
            properties=pika.BasicProperties(delivery_mode=2)
        )
    connection.close()
    print(" Envío completado.")

if __name__ == "__main__":
    send_test_batch(3000) # Enviamos 3000 de golpe