Auto Retry
==========

```python
retry_backoff = 9    # Max sec
max_retries   = 2    # Max tries
retry_jitter  = True # Random range(retry_backoff)
```

Retry 2, 6 secs
```bash
raised unexpected: Exception('Trying the retrying celery feature')
```

Manual Retry
============

```python
retry_backoff       = 9 # Does not apply
default_retry_delay = 3 # Use this instead
```

Do not throw
------------

Still raises my exception.
But internally it does not raise the **Retry** exception.
Thus, marking as failed if raised an exception or successful if returns after the **Retry** call


Task ACKS on Failure or Timeout
-------------------------------

```python
task_acks_on_failure_or_timeout = False
```

After retrying `max_retries` times, it abandons the message.
The message is not ACKed. Stays **unack**ed.
The worker does not take any new messages.


After the worker dies, the message goes back to **ready**.
The new worker only try once.
It seems number of retries stay linked to the id of the message.


