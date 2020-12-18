from arbiter import Arbiter
from time import sleep

arbiter = Arbiter(host="192.168.1.3", port=5672, user='user', password='password')

task_keys = arbiter.apply("add", task_args=[1, 2])
sleep(3)
for task_key in task_keys:
    print(arbiter.status(task_key))
sleep(3)
for task_key in task_keys:
    arbiter.kill(task_key)
for task_key in task_keys:
    print(arbiter.status(task_key))
arbiter.close()
