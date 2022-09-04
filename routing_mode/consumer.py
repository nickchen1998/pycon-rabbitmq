import sys
from utils.create_pika_conn import create_pika_conn


def callback(ch, method, properties, body):
    print(f" [x] Received:  {method.routing_key}  {body.decode()}")


with create_pika_conn() as connection:
    channel = connection.channel()

    channel.exchange_declare(exchange='logs', exchange_type='direct')

    result = channel.queue_declare(queue='')
    queue_name = result.method.queue

    levels = sys.argv[1:]
    for level in levels:
        channel.queue_bind(exchange='logs',
                           queue=queue_name,
                           routing_key=level)

    channel.basic_consume(queue=queue_name,
                          on_message_callback=callback,
                          auto_ack=True)

    try:
        print(' [*] Waiting for messages. To exit press CTRL+C')
        channel.start_consuming()
    except KeyboardInterrupt:
        print('Interrupted.')
        channel.stop_consuming()
