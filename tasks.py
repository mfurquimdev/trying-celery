from celery import Celery

from celery.contrib import rdb


import time

app_celery = Celery("tasks")
app_celery.config_from_object('celeryconfig')

@app_celery.task(bind=True,
                 name='tasks.add',
                 soft_time_limit=30,  # 3h to raise a SoftTimeLimitExceeded
                 time_limit=60,       # 4h to kill the process
                 throw=False,
                 max_retries=1,
#                 retry_backoff=9,
#                 retry_jitter=True,
                 default_retry_delay=1)
def add(self, x, y):

    print(f'Request ID: {self.request.id}')
    time.sleep(5)

    print(f"Received task: {x} + {y}")
    time.sleep(5)

    try:
        print(f"Result of {x} + {y} = {x+y}")
        time.sleep(5)

        raise Exception("Trying the retrying celery feature")

    except Exception as exc:
        raise self.retry(exc=exc)

    print(f"Return the result")
    return x + y

