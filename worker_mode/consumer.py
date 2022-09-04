import time
import random
from utils.create_pika_conn import create_pika_conn

collection_batch_size = 5
collection = []


def run(task):
    sleep_time = random.randint(1, 6)
    print('sleep time:', sleep_time, '\n')
    time.sleep(sleep_time)


def batch_run(tasks):
    print('tasks length:', len(tasks))
    print(tasks, '\n')

    rand_num = random.randint(0, 10)

    if rand_num == 0:
        print('failed.', '\n')
        return False
    else:
        print(tasks, '\n')
        time.sleep(3)
        return True


def callback(ch, method, properties, body):
    msg = body.decode()
    print(f" [*] Received: {msg}")
    time.sleep(2)

    # run(msg)


def batch_callback(ch, method, properties, body):
    msg = body.decode()
    print(f" [*] Received: {msg}")

    collection.append(msg)
    if len(collection) % collection_batch_size == 0:
        result = batch_run(collection)
        if result:
            ch.basic_ack(delivery_tag=method.delivery_tag, multiple=True)
        else:
            ch.basic_nack(delivery_tag=method.delivery_tag, multiple=True)
        collection.clear()


# 若需要啟動多個˙ consumer 則需要先啟動 consumer 後，再利用 producer 發送訊息
with create_pika_conn() as connection:
    channel = connection.channel()
    channel.queue_declare(queue='work-queue')

    # 當每個 consumer 處理完資料，才會去拿下一筆，才不會造成資源閒置
    # channel.basic_qos(prefetch_count=1)
    # channel.basic_consume(queue='work-queue',
    #                       on_message_callback=callback,
    #                       auto_ack=True)

    # 設定每個 consume 要接收到多少資料才會進行 callback
    channel.basic_qos(prefetch_count=collection_batch_size)
    channel.basic_consume(queue="work-queue",
                          on_message_callback=batch_callback)

    try:
        print(' [*] Waiting for messages. To exit press CTRL+C')
        channel.start_consuming()
    except KeyboardInterrupt:
        print('Stop consuming.')
        channel.stop_consuming()
