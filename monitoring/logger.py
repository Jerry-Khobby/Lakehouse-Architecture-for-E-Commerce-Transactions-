import logging
import sys

LOG_FORMAT  = "%(asctime)s  %(levelname)-8s  %(message)s"
DATE_FORMAT = "%H:%M:%S"


def buildLogger(name):
    logger = logging.getLogger(name)
    if logger.handlers:
        return logger

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(logging.Formatter(LOG_FORMAT, datefmt=DATE_FORMAT))
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    return logger
