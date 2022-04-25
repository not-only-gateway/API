import pika
from pika import exceptions


def consumer(callback, queue, host, user_name, user_password, port=5672, uri='/'):
    try:
        credentials = pika.PlainCredentials(user_name, user_password)
        params = pika.ConnectionParameters(host, port, uri, credentials)

        connection = pika.BlockingConnection(params)
        channel = connection.channel()
        channel.queue_declare(queue=queue)

        channel.basic_consume(queue=queue, on_message_callback=callback, auto_ack=True)
        channel.start_consuming()
        channel.close()
    except (exceptions.ProbableAccessDeniedError, exceptions.ChannelClosedByBroker):
        pass
