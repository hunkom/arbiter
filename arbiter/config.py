import logging

logging.basicConfig(
    level=logging.INFO,
    datefmt="%Y.%m.%d %H:%M:%S %Z",
    format="%(asctime)s - %(levelname)8s - %(name)s - %(message)s",
)

task_types = ["heavy", "light"]


class Config(object):
    def __init__(self, host, port, user, password, vhost,
                 light_queue, heavy_queue, all_queue):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.vhost = vhost
        self.light = light_queue
        self.heavy = heavy_queue
        self.all = all_queue
        self.worker_type = "heavy"  # default state
