#   Copyright 2020 getcarrier.io
#
#   Licensed under the Apache License, Version 2.0 (the "License");
#   you may not use this file except in compliance with the License.
#   You may obtain a copy of the License at
#
#       http://www.apache.org/licenses/LICENSE-2.0
#
#   Unless required by applicable law or agreed to in writing, software
#   distributed under the License is distributed on an "AS IS" BASIS,
#   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#   See the License for the specific language governing permissions and
#   limitations under the License.


import pika
from json import dumps
from uuid import uuid4
from time import sleep

from arbiter.config import Config
from arbiter.event.process import ProcessEventHandler


class ProcessWatcher:
    def __init__(self, process_id, host, port, user, password, vhost="carrier", all_queue="arbiterAll",
                 wait_time=2.0):
        self.config = Config(host, port, user, password, vhost, None, all_queue)
        self.connection = self._get_connection()
        self.process_id = process_id
        self.state = {}
        self.subscriptions = dict()
        self.arbiter_id = str(uuid4())
        self.handler = ProcessEventHandler(self.config, self.subscriptions, self.state, self.process_id)
        self.handler.start()
        self.handler.wait_running()
        self.wait_time = wait_time

    def _get_connection(self):  # This code duplication needed to avoid thread safeness problem of pika
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
        return channel

    def send_message(self, msg, queue="", exchange=""):
        self._get_connection().basic_publish(
            exchange=exchange, routing_key=queue,
            body=dumps(msg).encode("utf-8"),
            properties=pika.BasicProperties(
                delivery_mode=2
            )
        )

    def collect_state(self, tasks):
        if self.process_id not in self.state:
            self.state[self.process_id] = {
                "running": [],
                "done": []
            }
        message = {
            "type": "task_state",
            "tasks": tasks,
            "arbiter": self.process_id
        }
        self.send_message(message, exchange=self.config.all)
        sleep(self.wait_time)
        return self.state.get(self.process_id, {})

    def clear_state(self, tasks):
        message = {
            "type": "clear_state",
            "tasks": tasks,
            "arbiter": self.process_id
        }
        self.send_message(message, exchange=self.config.all)
        sleep(self.wait_time)

    def close(self):
        self.handler.stop()
        self._get_connection().queue_delete(queue=self.process_id)
        self.handler.join()
