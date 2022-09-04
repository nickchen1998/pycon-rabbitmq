from utils.create_pika_conn import create_pika_conn

with create_pika_conn() as connection:
    channel = connection.channel()

    channel.queue_declare(queue='work-queue')

    for i in range(1, 20 + 1):
        msg = bytes(i)
        channel.basic_publish(exchange='', routing_key='work-queue', body=msg)
        print(f' [x] Sent: {msg}')

    connection.close()
