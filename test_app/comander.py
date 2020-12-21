from arbiter import Arbiter

arbiter = Arbiter(host="192.168.1.3", port=5672, user='user', password='password')

print(arbiter.workers())

task_keys = arbiter.apply("add", task_args=[1, 2])

for task_key in task_keys:
    print(arbiter.status(task_key))

# for task_key in task_keys:
#     arbiter.kill(task_key)

for message in arbiter.wait_for_tasks(task_keys):
    print(message)

for task_key in task_keys:
    print(arbiter.status(task_key))

arbiter.close()
