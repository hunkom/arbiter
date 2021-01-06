import json
import logging

from arbiter.event.base import BaseEventHandler


class GlobalEventHandler(BaseEventHandler):

    def _connect_to_specific_queue(self, channel):
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

        logging.info("Waiting for global events")
        return channel

    def queue_event_callback(self, channel, method, properties, body):
        """ Process event """
        _ = properties, self, channel, method
        logging.info("[GlobalEvent] Got event")
        try:
            event = json.loads(body)
            #
            event_type = event.get("type", None)
            logging.info("[GlobalEvent] Type: %s", event_type)
            #
            if event_type in ["stop_task", "purge_task"]:
                task_key = event.get("task_key")
                logging.info(f"!!!!!!!!!!!!!!!!!! Task key: {task_key}")
                logging.info(f"!!!!!!!!!!!!!!!!!! State: {self.state}")
                if task_key in self.state and self.state[task_key].is_alive():
                    logging.info("[GlobalEvent] Terminating task %s", task_key)
                    self.state[task_key].terminate()
            elif event_type == "subscription_notification":
                subscription = event.get("subscription")
                if subscription in self.subscriptions:
                    logging.info("[GlobalEvent] Got data for subscription %s", subscription)
                    self.subscriptions[subscription] = event.get("data")
            elif event_type == "state":
                self.respond(channel, {"active": self.state["active_workers"],  "total": self.state["total_workers"],
                                       "available": self.state["total_workers"] - self.state["active_workers"],
                                       "type": "state", "worker": self.state["type"]}, event["arbiter"])
        except:  # pylint: disable=W0702
            logging.exception("[GlobalEvent] Got exception")
