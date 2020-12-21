#
#
#  Simple Task add x+y
#
#

from arbiter import Minion
import logging
from time import sleep

app = Minion(host="192.168.1.3", port=5672, user='user', password='password')


@app.task(name="add")
def add(x, y):
    logging.info("Running task 'add'")
    sleep(10)
    # task that initiate new task within same app
    for message in app.add_task('simple_add', task_args=[3, 4], sync=True):
        logging.info(message)
    logging.info("sleep done")
    return x+y


@app.task(name="simple_add")
def adds(x, y):
    logging.info("Running task 'add_small'")
    return x + y


#
#  Complicated task with
#  Running container and passing params to container
#
@app.task(name="lambda")
def faas(runtime="", artifact="/", galloper='', auth_token='', lambda_env=[], entrypoint=''):
    pass


if __name__ == "__main__":
    app.run(worker_type="heavy", workers=3)
