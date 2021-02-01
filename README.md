# trying-celery

Execute the following to spawn a worker

```bash
$ celery -A tasks worker --loglevel=INFO
```

Execute the following to submit a task

```bash
$ python request.py
```
