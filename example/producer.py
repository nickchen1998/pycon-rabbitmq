import uuid
from pika import BlockingConnection
from utils.create_pika_conn import create_pika_conn

connection: BlockingConnection = create_pika_conn()
channel = connection.channel()

channel.queue_declare(queue='tutorial')

for _ in range(20):

    msg = str(uuid.uuid4())
    channel.basic_publish(exchange='', routing_key='tutorial', body=msg)
    print(f' [x] Sent: {msg}')

connection.close()
