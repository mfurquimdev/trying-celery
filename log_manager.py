import logging
import sys
import parameter

logging_level = {
    'DEBUG': logging.DEBUG,
    'INFO': logging.INFO,
    'WARNING': logging.WARNING,
    'ERROR': logging.ERROR,
    'CRITICAL': logging.CRITICAL
}


class Logging:

    def __init__(self):
        self.root = logging.getLogger()
        self.root.setLevel(logging_level.get(parameter.get_env('LOG_LEVEL')))

        self.ch = logging.StreamHandler(sys.stdout)
        self.ch.setLevel(logging_level.get(parameter.get_env('LOG_LEVEL')))
        self.formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s [%(process)d]: %(message)s')
        self.ch.setFormatter(self.formatter)
        self.root.addHandler(self.ch)

    def debug(self, text):
        self.root.debug(str(text))

    def info(self, text):
        self.root.info(str(text))

    def warning(self, text):
        self.root.warning(str(text))

    def error(self, text):
        self.root.error(str(text))

    def critical(self, text):
        self.root.critical(str(text))

    def exception(self, text):
        self.root.exception(str(text))


log = Logging()

# Set cassandra log level
logging.getLogger('cassandra').setLevel(
    logging_level.get(parameter.get_env('LOG_LEVEL')))

# Set Pika log level
logging.getLogger("pika").setLevel(
    logging_level.get(parameter.get_env('LOG_LEVEL')))

# Set mlms log level
logging.getLogger('mlms').setLevel(
    logging_level.get(parameter.get_env('LOG_LEVEL')))
