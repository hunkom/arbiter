import json
import logging

from arbiter.event.base import BaseEventHandler


class ProcessEventHandler(BaseEventHandler):
    def _connect_to_specific_queue(self, channel):
        channel.queue_declare(
            queue=self.settings.queue, durable=True
        )
        exchange_queue = channel.queue_declare(queue="", exclusive=True)
        channel.queue_bind(
            exchange=self.settings.all,
            queue=exchange_queue.method.queue
        )
        channel.basic_consume(
            queue=exchange_queue.method.queue,
            on_message_callback=self.queue_event_callback,
            auto_ack=True
        )
        return channel

    def queue_event_callback(self, channel, method, properties, body):
        """ Process event """
        _ = properties, self, channel, method
        try:
            event = json.loads(body)
            event_type = event.get("type", None)
            if event_type == "subscription_notification":
                subscription = event.get("subscription")
                if subscription in self.subscriptions:
                    self.subscriptions[subscription] = event.get("data")
        except:  # pylint: disable=W0702
            logging.exception("[ProcessEvent] Got exception")
