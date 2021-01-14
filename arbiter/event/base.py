import time
import json
import threading
import pika
import logging
from arbiter.rabbit_connector import _get_connection


class BaseEventHandler(threading.Thread):
    """ Basic representation of events handler"""

    def __init__(self, settings, subscriptions, state):
        super().__init__(daemon=True)
        self.settings = settings
        self.state = state
        self.subscriptions = subscriptions
        self._stop_event = threading.Event()

    def _connect_to_specific_queue(self, channel):
        raise NotImplemented

    def run(self):
        """ Run handler thread """
        logging.info("Starting handler thread")
        while not self.stopped():
            logging.info("Starting handler consuming")
            try:
                channel = _get_connection(self.settings)
                channel = self._connect_to_specific_queue(channel)
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

    @staticmethod
    def respond(channel, message, queue):
        logging.debug(message)
        channel.basic_publish(
            exchange="", routing_key=queue,
            body=json.dumps(message).encode("utf-8"),
            properties=pika.BasicProperties(
                delivery_mode=2
            )
        )

    def queue_event_callback(self, channel, method, properties, body):  # pylint: disable=R0912,R0915
        raise NotImplemented
