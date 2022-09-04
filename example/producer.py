import uuid
from utils.create_pika_conn import create_pika_conn

with create_pika_conn() as connection:
    channel = connection.channel()

    channel.queue_declare(queue='tutorial')

    for i in range(20):
        msg = bytes(str(uuid.uuid4()), 'utf-8')
        channel.basic_publish(exchange='', routing_key='tutorial', body=msg)
        print(f' [x] Sent: {msg}')
