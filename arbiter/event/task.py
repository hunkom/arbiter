import json
import logging
import pika
from uuid import uuid4
from traceback import format_exc

from arbiter.event.base import BaseEventHandler
from arbiter.task.processor import TaskProcess


class TaskEventHandler(BaseEventHandler):
    def __init__(self, settings, subscriptions, state, task_registry):
        super().__init__(settings, subscriptions, state)
        self.task_registry = task_registry

    def _connect_to_specific_queue(self, channel):
        channel.basic_qos(prefetch_count=1)
        channel.basic_consume(
            queue=self.settings.__getattribute__(self.settings.worker_type),
            on_message_callback=self.queue_event_callback
        )
        logging.info("[%s] Waiting for task events", self.ident)
        return channel

    def queue_event_callback(self, channel, method, properties, body):  # pylint: disable=R0912,R0915
        _ = properties, self, channel, method
        logging.info("[%s] [TaskEvent] Got event", self.ident)
        event = json.loads(body)
        try:
            # new tasks will be w/o key testing purpose only
            # do not use in prod implementation
            if not event.get("task_key"):
                event["task_key"] = uuid4()
            event_type = event.get("type", "task")
            logging.info("[%s] [TaskEvent] Type: %s", self.ident, event_type)
            if event_type == "task":
                if event.get("arbiter"):
                    channel.basic_publish(
                        exchange="", routing_key=event.get("arbiter"),
                        body=json.dumps({
                            "type": "task_state_change",
                            "task_key": event.get("task_key"),
                            "task_state": "running"
                        }).encode("utf-8"),
                        properties=pika.BasicProperties(
                            delivery_mode=2
                        )
                    )
                logging.info("[%s] [TaskEvent] Starting worker process", self.ident)
                if event.get("task_name") not in self.task_registry:
                    raise ModuleNotFoundError("Task is not a part of this worker")
                worker = TaskProcess(
                    self.settings, self.subscriptions, self.task_registry[event.get("task_name")],
                    event.get("task_name"), event.get("task_key"), event.get("args", []),
                    event.get("kwargs", {})
                )
                self.state[event.get("task_key")] = worker
                worker.start()
                while worker.is_alive():
                    channel._connection.sleep(1.0)  # pylint: disable=W0212
                task_result = worker.result()
                self.state.pop(event.get("task_key"))
                logging.info("[%s] [TaskEvent] Worker process stopped", self.ident)
                if event.get("arbiter"):
                    channel.basic_publish(
                        exchange="", routing_key=event.get("arbiter"),
                        body=json.dumps({
                            "type": "task_state_change",
                            "task_key": event.get("task_key"),
                            "result": task_result,
                            "task_state": "done"
                        }).encode("utf-8"),
                        properties=pika.BasicProperties(
                            delivery_mode=2
                        )
                    )
        except:
            logging.exception("[%s] [TaskEvent] Got exception", self.ident)
            if event.get("arbiter"):
                channel.basic_publish(
                    exchange="", routing_key=event.get("arbiter"),
                    body=json.dumps({
                        "type": "task_state_change",
                        "task_key": event.get("task_key"),
                        "result": format_exc(),
                        "task_state": "exception"
                    }).encode("utf-8"),
                    properties=pika.BasicProperties(
                        delivery_mode=2
                    )
                )
        channel.basic_ack(delivery_tag=method.delivery_tag)
