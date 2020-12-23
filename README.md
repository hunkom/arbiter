# Arbiter
Distributed tasks queue use RabbitMQ as broker. Consists of arbiter and minion.

## Installation

Clone git repo
```bash
git clone https://github.com/carrier-io/arbiter.git 
```
and install by running
```bash
cd arbiter
python setup.py install
```

## Basic scenario

Launch RabbitMQ, as it required for everything to work
```bash
docker run -d --rm --hostname arbiter-rabbit --name arbiter-rabbit \
           -p 5672:5672 -p 15672:15672 -e RABBITMQ_DEFAULT_USER=user \
           -e RABBITMQ_DEFAULT_PASS=password \
           -e RABBITMQ_DEFAULT_VHOST=carrier \
           rabbitmq:3-management
```

### Create simple task
You need to initiate minion and provide connection details:
```python
from arbiter import Minion

app = Minion(host="localhost", port=5672, 
             user='user', password='password')
```
then you can declare tasks by decorating callable with `@app.task`
```python
@app.task(name="simple_add")
def adds(x, y):
    return x + y
```
Every task need to have a name, which it will be referred by when initiated from arbiter.
this is pretty much it to create first task

Now we need to create execution point
```python
if __name__ == "__main__":
    app.run(worker_type="heavy", workers=3)
```
where `worker_type` can be either light or heavy, and quantity of worker slots to do the job(s) 

Run created script. Minion is ready to accept work orders.

example of minion can be found at `test_app\minion.py`

### Call created task from arbiter
Arbiter is job initiator, it maintain the state of all jobs it created and can retrieve results.

Each arbiter have it's own communication channel, so job results won't mess between two different arbiters

Declaring the arbiter
```python
from arbiter import Arbiter
arbiter = Arbiter(host='localhost', port=5672, user='user', password='password')
``` 
to call the task and track it till it done (tasks are obviously async)
```python
task_keys = arbiter.apply("simple_add", tasks_count=1, task_args=[1, 2]) #  will return array of task ids

# while loop with returns results of each task once it done
for message in arbiter.wait_for_tasks(task_keys):
    print(message)
```
Alternatively you can get task result by calling
```python
arbiter.status(task_keys[0])
```
it will return `json` where `result` will be one of the keys

Example of arbiter can be found in `test_app/comander.py`