import time
from pika import BlockingConnection, BasicProperties, spec
from utils.create_pika_conn import create_pika_conn


# ==================== produce message ====================
# with create_pika_conn() as connection:
#     channel = connection.channel()
#     channel.queue_declare(queue='hello')
#
#     # 發送單筆資料
#     msg = b'hello world!'
#     channel.basic_publish(exchange='', routing_key='hello', body=msg)
#     print(f' [x] Sent: {msg}')
#
#     # 發送多筆資料
#     for i in range(1, 20 + 1):
#         msg = bytes(i)
#         channel.basic_publish(exchange='', routing_key='hello', body=msg)
#         print(f' [x] Sent: {msg}')

# ==================== keep message in queue ====================
# with create_pika_conn() as connection:
#     channel = connection.channel()
#
#     # 此寫法可以保證 rabbitmq 在關閉後 queue 不會被刪除，但 queue 內資料會刪除
#     channel.queue_declare(queue="hello", durable=True)
#
#     # 下面這個 publish 資料的寫法，可以保證 queue 關閉時，資料不會被刪除
#     channel.basic_publish(exchange="",
#                           routing_key="hello",
#                           body=b"hello",
#                           properties=BasicProperties(delivery_mode=spec.PERSISTENT_DELIVERY_MODE))


# ==================== consume message [1] ====================
# with create_pika_conn() as connection:
#     channel = connection.channel()
#     channel.queue_declare(queue='hello')
#
#     # 若 basic_get 內沒有其他確認參數，則資料取出後並不會從 queue 內刪除
#     # method, properties, body = channel.basic_get(queue='hello')
#
#     # auto_ack=True，則會協助確認訊息機制，成功取出資料後則會刪除
#     method, properties, body = channel.basic_get(queue='hello', auto_ack=True)
#
#     # 或是在取出訊息後使用下方程式確認
#     # channel.basic_ack(method.delivery_tag)
#
#     print(f' [x] Received: {body.decode()}')


# ==================== consume message [2] ====================
# 此方法會不斷監聽指定的 queue 直到手動進行 KeyboardInterrupt
# def callback(ch, method, properties, body):
#     print(f" [x] Received {body.decode()}")
#     time.sleep(1)
#
#
# with create_pika_conn() as connection:
#     channel = connection.channel()
#     channel.queue_declare(queue='hello')
#
#     # 利用 on_message_callback 參數設定 return 後的資料該座甚麼處理
#     channel.basic_consume(queue='hello', on_message_callback=callback)
#
#     try:
#         print(' [*] Waiting for messages. To exit press CTRL+C')
#         channel.start_consuming()
#     except KeyboardInterrupt:
#         print('Interrupted.')
#         channel.stop_consuming()

# ==================== consume message [3] ====================

# channel.queue_declare(queue='hello')

# for method, properties, body in channel.consume(queue='hello', inactivity_timeout=10):

#     print(f" [x] Received {body.decode()}")
#     time.sleep(1)

#     if method == None and properties == None and body == None:
#         break

# connection.close()
