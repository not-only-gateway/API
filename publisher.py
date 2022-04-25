import pika, json
from pika import exceptions


def publish(method, body, routing, host, user_name, user_password, port=5672, uri='/'):
    try:
        credentials = pika.PlainCredentials(user_name, user_password)
        params = pika.ConnectionParameters(host, port, uri, credentials)
        connection = pika.BlockingConnection(params)
        channel = connection.channel()
        properties = pika.BasicProperties(method)
        channel.basic_publish(exchange='', routing_key=routing, body=json.dumps(body), properties=properties)
    except (
            exceptions.ProbableAccessDeniedError,
            exceptions.ChannelClosedByBroker,
            exceptions.StreamLostError,
            exceptions.AMQPConnectionError
    ):
        pass
