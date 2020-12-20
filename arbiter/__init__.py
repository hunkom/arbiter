import logging
import pika

from time import sleep
from arbiter.event.task import TaskEventHandler
from arbiter.event.broadcast import GlobalEventHandler
from arbiter.config import Config, task_types


class Minion:
    def __init__(self, host, port, user, password, vhost="carrier",
                 light_queue="arbiterLight", heavy_queue="arbiterHeavy", all_queue="arbiterAll"):
        self.config = Config(host, port, user, password, vhost, light_queue, heavy_queue, all_queue)
        self.task_registry = {}

    def task(self, *args, **kwargs):
        """ Task decorator """
        def inner_task(func):
            def create_task(**kwargs):
                def _create_task(func):
                    return self._create_task_from_callable(func, **kwargs)
                return _create_task
            if callable(func):
                return create_task(**kwargs)(func)
            raise TypeError('@task decorated function must be callable')
        return inner_task

    def _create_task_from_callable(self, func, name=None, task_type="heavy", **kwargs):
        name = name if name else f"{func.__name__}.{func.__module__}"
        if task_type not in task_types:
            raise TypeError(f'task_type is not supported please use any of [{",".join(task_types)}]')
        if name not in self.task_registry:
            self.task_registry[name] = func
        return self.task_registry[name]

    def run(self, worker_type, workers):
        state = dict()
        subscriptions = dict()
        self.config.worker_type = worker_type
        logging.info("Starting '%s' worker", self.config.worker_type)
        # Listen for task events
        state["total_workers"] = workers
        state["active_workers"] = 0
        for _ in range(workers):
            TaskEventHandler(self.config, subscriptions, state, self.task_registry).start()
        # Listen for global events
        global_handler = GlobalEventHandler(self.config, subscriptions, state)
        global_handler.start()
        global_handler.join()


from uuid import uuid4
from json import dumps
from arbiter.event.arbiter import ArbiterEventHandler
connection = None


class Arbiter:
    # ToDo: inmemory sqlite for state
    def __init__(self, host, port, user, password, vhost="carrier", light_queue="arbiterLight",
                 heavy_queue="arbiterHeavy", all_queue="arbiterAll", start_consumer=True):
        self.config = Config(host, port, user, password, vhost, light_queue, heavy_queue, all_queue)
        self.arbiter_id = None
        self.state = dict()
        self.subscriptions = dict()
        self.handler = None
        if start_consumer:
            self.arbiter_id = str(uuid4())
            self.handler = ArbiterEventHandler(self.config, self.subscriptions, self.state, self.arbiter_id)
            self.handler.start()

    def _get_connection(self):
        global connection
        if not connection:

            _connection = pika.BlockingConnection(
                pika.ConnectionParameters(
                    host=self.config.host,
                    port=self.config.port,
                    virtual_host=self.config.vhost,
                    credentials=pika.PlainCredentials(
                        self.config.user,
                        self.config.password
                    )
                )
            )
            channel = _connection.channel()
            channel.queue_declare(
                queue=self.config.light, durable=True
            )
            channel.queue_declare(
                queue=self.config.heavy, durable=True
            )
            channel.exchange_declare(
                exchange=self.config.all,
                exchange_type="fanout", durable=True
            )
            connection = channel
        return connection

    def send_message(self, msg, queue="", exchange=""):
        self._get_connection().basic_publish(
            exchange=exchange, routing_key=queue,
            body=dumps(msg).encode("utf-8"),
            properties=pika.BasicProperties(
                delivery_mode=2
            )
        )

    def apply(self, task_name, task_type="heavy", tasks_count=1, task_args=None, task_kwargs=None):
        if not task_args:
            task_args = []
        if not task_kwargs:
            task_kwargs = {}
        message = {
            "type": "task",
            "task_name": task_name,
            "task_key": "",
            "args": task_args,
            "kwargs": task_kwargs,
            "arbiter": self.arbiter_id
        }
        tasks = []
        for _ in range(tasks_count):
            task_key = str(uuid4())
            tasks.append(task_key)
            message["task_key"] = task_key
            self.state[task_key] = {
                "task_type": task_type,
                "state": "initiated"
            }
            self.send_message(message, queue=self.config.__getattribute__(task_type))
        return tasks

    def kill(self, task_key):
        message = {
            "type": "stop_task",
            "task_key": task_key,
            "arbiter": self.arbiter_id
        }
        self.send_message(message, exchange=self.config.all)
        while True:
            if self.state[task_key]["state"] == "done":
                break

    def status(self, task_key):
        return self.state[task_key]["state"]

    def close(self):
        self.handler.stop()
        self._get_connection().queue_delete(queue=self.arbiter_id)
        self.handler.join()

    def workers(self):
        message = {
            "type": "state",
            "arbiter": self.arbiter_id
        }
        if "state" in self.state:
            del self.state["state"]
        self.send_message(message, exchange=self.config.all)
        sleep(2)
        return self.state["state"]
