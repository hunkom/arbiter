import pika
import logging

connection = None


def _get_connection(config):
    global connection
    logging.info(f"!!!!!!!!!!!!Connection: {connection}")
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
        channel.exchange_declare(
            exchange=config.all,
            exchange_type="fanout", durable=True
        )
        connection = channel
    try:
        connection._process_data_events(time_limit=0)
    except (pika.exceptions.StreamLostError, pika.exceptions.ChannelClosedByBroker):
        logging.info("!!!!!!!!!!!!!!!!!!!!!Got exception in _get_connection method")
        connection = None
        return _get_connection(config)

    return connection
