from arbiter import Minion
import logging
from time import sleep

app = Minion(host="192.168.1.3", port=5672, user='user', password='password')


@app.task(name="add")
def add(x, y):
    logging.info("Running task 'add'")
    sleep(30)
    logging.info("sleep done")
    return x+y


if __name__ == "__main__":
    app.run(worker_type="heavy", workers=3)
