from celery import Celery

import time

app_celery = Celery("tasks")
app_celery.config_from_object('celeryconfig')

@app_celery.task(name='tasks.add')
def add(x, y):
    print(f"Received task: {x} + {y}")
    time.sleep(10)
    print(f"Result of {x} + {y} = {x+y}")
    time.sleep(10)
    print(f"Return the result")
    return x + y

