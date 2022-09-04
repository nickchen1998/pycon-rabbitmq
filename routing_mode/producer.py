import time
import random
from utils.create_pika_conn import create_pika_conn

with create_pika_conn() as connection:
    channel = connection.channel()

    channel.exchange_declare(exchange='logs', exchange_type='direct')

    logs = [
        'This is an INFO message.',
        'This is a WARNING message.',
        'This is an ERROR message.'
    ]
    log_list = [random.choice(logs) for _ in range(20)]

    for i, log in enumerate(log_list):
        msg = f'({i}){log}'

        # 根據訊息不同，串進去不同的 route 中
        if 'INFO' in msg:
            channel.basic_publish(exchange='logs',
                                  routing_key='info',
                                  body=bytes(msg))
        if 'WARNING' in msg:
            channel.basic_publish(exchange='logs',
                                  routing_key='warning',
                                  body=bytes(msg))
        if 'ERROR' in msg:
            channel.basic_publish(exchange='logs',
                                  routing_key='error',
                                  body=bytes(msg))

        print(f' [x] Sent: {msg}')
        time.sleep(0.5)
