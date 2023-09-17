from kombu import Exchange, Queue


####### ATTENTION ########
# This is workaround for celery connection reset with rabbitMQ
# This issue appears to be solved in celery 4.3 by default
# TODO: maybe remove when using celery >4.3
broker_pool_limit = None
broker_connection_max_retries = None

# Logging
worker_redirect_stdouts = True
worker_redirect_stdouts_level = "debug"
worker_log_format = '%(asctime)s - %(levelname)s [%(process)d]: %(message)s'
worker_task_log_format = '%(asctime)s - %(levelname)s [%(process)d]: %(task_name)s (%(task_id)s): %(message)s'

# Configure the broker URI
broker_url = "pyamqp://retro:retro@localhost:5672/0"

# TODO Convert queue names to a parameter configuration
task_default_queue = 'tasks'

# Configure logs timezones
timezone = 'UTC'
enable_utc = True

#result_backend = "redis://localhost:6379/3"

# In case a worker is lost force celery to wait some seconds before
# recreating, this is necessary to prevent published result to be lost
worker_lost_wait = 120.0

# Configure the number of tasks a worker can consume before being
# replaced by a new one.
# This is necessary to recycle the workers and prevent any memory
# accumulation from leaks
worker_max_tasks_per_child = 1

# Configure the number of tasks a worker can reserve for itself
# This is necessary to prevent the worker from reserving long tasks
# to itself, preventing other workers to work on then.
worker_prefetch_multiplier = 1
worker_concurrency = 1

# Configure the memory threshold for recycling the current worker-child
# When the memory consumption of a task get higher than this limit,
# The worker will wait for the task to finish and replace the worker-child
worker_max_memory_per_child = 1000000  # 1 GB

# Configure the file to persist state database, the default operation uses memory
# and can be overwritten when service restarts. This is usefull to persist task revoke
worker_state_db = 'celery_states.db'

# Force celery to just acknowledge the task after it is executed
task_acks_late = True

# Force celery to not acknowledge the tasks on failure of timeout
# This will cause the tasks to be rerun.
task_acks_on_failure_or_timeout = False

# Even if acks_late is enabled, the worker will acknowledge tasks when the worker process executing them abruptly exits
# or is signaled (e.g., KILL/INT, etc). Setting this to true allows the message to be re-queued instead, so that the
# task will execute again by the same worker, or another worker.
# Warning: Enabling this can cause message loops; make sure you know what you’re doing.
# TODO: Uncoment to test
#task_reject_on_worker_lost = True

#Celery broker connection configs
broker_connection_timeout = 10
broker_connection_retry = True
broker_connection_max_retries = 100
