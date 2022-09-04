import time
from pika import BlockingConnection, BasicProperties, spec
from utils.create_pika_conn import create_pika_conn


# ==================== produce message ====================

# with create_pika_conn() as connection:
#     channel = connection.channel()
#
#     channel.queue_declare(queue='hello')
#
#     msg = b'hello world!'
#     channel.basic_publish(exchange='', routing_key='hello', body=msg)
#     print(f' [x] Sent: {msg}')

# 這邊不知道要幹嘛用，先暫時放著
# for i in range(1,20+1):

#     msg = str(i)
#     channel.basic_publish(exchange='', routing_key='hello', body=msg)
#     print(f' [x] Sent: {msg}')


# ==================== consume message [1] ====================
# 此方式取資料不會讓資料從 queue 內移除
# with create_pika_conn() as connection:
#     channel = connection.channel()
#     channel.queue_declare(queue='hello')
#
#     method, properties, body = channel.basic_get(queue='hello')
#     print(f' [x] Received: {body.decode()}')

# ==================== keep message in queue ====================
with create_pika_conn() as connection:
    channel = connection.channel()

    # 此寫法可以保證 rabbitmq 在關閉後 queue 不會被刪除，但 queue 內資料會刪除
    channel.queue_declare(queue="hello", durable=True)

    # 下面這個 publish 資料的寫法，可以保證 queue 關閉時，資料不會被刪除
    channel.basic_publish(exchange="",
                          routing_key="hello",
                          body=b"hello",
                          properties=BasicProperties(delivery_mode=spec.PERSISTENT_DELIVERY_MODE))


# ==================== consume message [2] ====================

# channel.queue_declare(queue='hello')

# def callback(ch, method, properties, body):

#     print(f" [x] Received {body.decode()}")
#     time.sleep(1)

# channel.basic_consume(queue='hello', on_message_callback=callback)

# try:
#     print(' [*] Waiting for messages. To exit press CTRL+C')
#     channel.start_consuming()
# except KeyboardInterrupt:
#     print('Interrupted.')
#     channel.stop_consuming()

# connection.close()


# ==================== consume message [3] ====================

# channel.queue_declare(queue='hello')

# for method, properties, body in channel.consume(queue='hello', inactivity_timeout=10):

#     print(f" [x] Received {body.decode()}")
#     time.sleep(1)

#     if method == None and properties == None and body == None:
#         break

# connection.close()
