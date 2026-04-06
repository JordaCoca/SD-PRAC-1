import pika
import json

def send_purchase_request(client_id, seat_id, request_id):
    # 1. Conexión a RabbitMQ
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()

    # 2. Declarar la cola (Durable=True para que no se pierda si Rabbit cae)
    channel.queue_declare(queue='ticket_queue', durable=True)

    # 3. Crear el mensaje
    message = {
        "client_id": client_id,
        "seat_id": seat_id,
        "request_id": request_id
    }

    # 4. Publicar
    channel.basic_publish(
        exchange='',
        routing_key='ticket_queue',
        body=json.dumps(message),
        properties=pika.BasicProperties(
            delivery_mode=2,  # Hace que el mensaje sea persistente en disco
        )
    )
    connection.close()