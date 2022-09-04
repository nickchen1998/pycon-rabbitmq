import time
from utils.create_pika_conn import create_pika_conn


def callback(ch, method, properties, body):
    print(f" [x] Received {body.decode()}")
    channel.basic_ack(method.delivery_tag)
    time.sleep(1)


with create_pika_conn() as connection:
    channel = connection.channel()
    channel.queue_declare(queue='tutorial')
    channel.basic_consume(queue='tutorial', on_message_callback=callback)

    try:
        print(' [*] Waiting for messages. To exit press CTRL+C')
        channel.start_consuming()
    except KeyboardInterrupt:
        print('Interrupted.')
        channel.stop_consuming()
