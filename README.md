# trying-celery

Execute the following to spawn a worker

```bash
$ celery -A tasks worker --loglevel=INFO
```

Execute the following to submit a task

```bash
$ python request.py
```



The following settings have been tested using celery version 4.3.0 and rabbitmq version 3.8.11.

When executing a celery worker with `task_acks_late = False`, an ack is sent immediately after the woker has taken a message on the queue.
When the worker is unexpectedly terminated (e.g. SIGKILL) before publishing the result, the message will not go back to the queue

On the other hand, when executing a celery worker with `task_acks_late = True`, an ack is sent only after the woker has processed the message.
When the worker is unexpectedly terminated (e.g. SIGKILL) before publishing the result, the message will not have been acked.
Thus, the message will return to the queue.

On both cases, when a SIGTERM is sent, the worker finishes the task and publishes the result.

Neither `task_acks_on_failure_or_timeout` nor `task_reject_on_worker_lost` seem to have any effect on the status of the queue when killing the worker.

