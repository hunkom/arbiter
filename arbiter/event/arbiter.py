import json
import logging

from arbiter.event.base import BaseEventHandler


class ArbiterEventHandler(BaseEventHandler):
    def __init__(self, settings, subscriptions, state, arbiter_id):
        super().__init__(settings, subscriptions, state)
        self.arbiter_id = arbiter_id

    def _connect_to_specific_queue(self, channel):
        channel.queue_declare(
            queue=self.arbiter_id, durable=True
        )
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(
            queue=self.arbiter_id,
            on_message_callback=self.queue_event_callback
        )
        logging.info("[%s] Waiting for task events", self.ident)
        return channel

    def queue_event_callback(self, channel, method, properties, body):  # pylint: disable=R0912,R0915
        _ = properties, self, channel, method
        logging.info("[%s] [TaskEvent] Got event", self.ident)
        event = json.loads(body)
        try:
            event_type = event.get("type")
            logging.info("[%s] [TaskEvent] Type: %s", self.ident, event_type)
            if event.get("task_key") not in self.state:
                self.state[event.get("task_key")] = {}
            if event_type in ["task_state_change"]:
                self.state[event.get("task_key")]["state"] = event.get("task_state")
                logging.info(f"Task state changed: {json.dumps(self.state[event.get('task_key')])}")
        except:
            logging.exception("[%s] [TaskEvent] Got exception", self.ident)
        channel.basic_ack(delivery_tag=method.delivery_tag)
