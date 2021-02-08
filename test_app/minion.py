from arbiter import Minion
import logging
from time import sleep

app = Minion(host="localhost", port=5672, user='user', password='password', queue="test")


@app.task(name="add")
def add(x, y):
    logging.info("Running task 'add'")
    # task that initiate new task within same app
    increment = 0
    for message in app.apply('simple_add', task_args=[3, 4]):
        if isinstance(message, dict):
            increment = message["result"]
    logging.info("sleep done")
    return x + y + increment


@app.task(name="simple_add")
def adds(x, y):
    logging.info(f"Running task 'add_small' with params {x}, {y}")
    sleep(30)
    return x + y


@app.task(name="add_in_pipe")
def addp(x, y, upstream=0):
    logging.info("Running task 'add_in_pipe'")
    return x + y + upstream


if __name__ == "__main__":
    app.run(workers=7)
    # app.rpc(workers=3, blocking=True)
