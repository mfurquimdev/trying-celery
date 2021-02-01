from tasks import add

result = add.delay(1,1)


try:
    result.get(timeout=1)
except TimeoutError:
    result.traceback

