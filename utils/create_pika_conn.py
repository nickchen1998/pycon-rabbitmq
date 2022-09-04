import pika
import contextlib


@contextlib.contextmanager
def create_pika_conn() -> pika.BlockingConnection:
    RABBITMQ_HOST = 'localhost'
    RABBITMQ_PORT = 5672
    RABBITMQ_USER = 'root'
    RABBITMQ_PASS = '1234'

    # connection & channel
    credentials = pika.PlainCredentials(RABBITMQ_USER, RABBITMQ_PASS)
    parameters = pika.ConnectionParameters(host=RABBITMQ_HOST,
                                           port=RABBITMQ_PORT,
                                           credentials=credentials)
    connection = pika.BlockingConnection(parameters)

    yield connection

    connection.close()
