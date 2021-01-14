import pika

connection = None


def _get_connection(config):
    global connection
    if not connection:
        _connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=config.host,
                port=config.port,
                virtual_host=config.vhost,
                credentials=pika.PlainCredentials(
                    config.user,
                    config.password
                )
            )
        )
        channel = _connection.channel()
        channel.queue_declare(
            queue=config.light, durable=True
        )
        channel.queue_declare(
            queue=config.heavy, durable=True
        )
        channel.exchange_declare(
            exchange=config.all,
            exchange_type="fanout", durable=True
        )
        # except pika.exceptions.StreamLostError:
        connection = channel
        return connection
