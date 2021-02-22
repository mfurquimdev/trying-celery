from celery import Celery

app = Celery('proj',
             backend='rpc://',
             broker='pyamqp://guest@localhost//',
             include=['proj.tasks'])

result_backend = 'redis://@localhost:6379/db'
app.conf.update(
    result_expires=3600,
)

if __name__ == '__main__':
    app.start()
