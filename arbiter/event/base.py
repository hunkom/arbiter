import time
import threading
import pika
import logging


class BaseEventHandler(threading.Thread):
    """ Basic representation of events handler"""

    def __init__(self, settings, subscriptions, state):
        super().__init__(daemon=True)
        self.settings = settings
        self.state = state
        self.subscriptions = subscriptions
        self._stop_event = threading.Event()

    def _get_channel(self):
        connection = pika.BlockingConnection(
            pika.ConnectionParameters(
                host=self.settings.host,
                port=self.settings.port,
                virtual_host=self.settings.vhost,
                credentials=pika.PlainCredentials(
                    self.settings.user,
                    self.settings.password
                )
            )
        )
        channel = connection.channel()
        channel.queue_declare(
            queue=self.settings.light, durable=True
        )
        channel.queue_declare(
            queue=self.settings.heavy, durable=True
        )
        channel.exchange_declare(
            exchange=self.settings.all,
            exchange_type="fanout", durable=True
        )
        channel = self._connect_to_specific_queue(channel)
        return channel

    def _connect_to_specific_queue(self, channel):
        raise NotImplemented

    def run(self):
        """ Run handler thread """
        logging.info("Starting handler thread")
        while not self.stopped():
            logging.info("Starting handler consuming")
            try:
                channel = self._get_channel()
                logging.info("[%s] Waiting for task events", self.ident)
                channel.start_consuming()
            except pika.exceptions.ConnectionClosedByBroker:
                break
            except pika.exceptions.AMQPChannelError:
                break
            except pika.exceptions.AMQPConnectionError:
                logging.info("Recovering from error")
                time.sleep(3.0)
                continue
        channel.stop_consuming()

    def stop(self):
        self._stop_event.set()

    def stopped(self):
        return self._stop_event.is_set()

    def queue_event_callback(self, channel, method, properties, body):  # pylint: disable=R0912,R0915
        raise NotImplemented
