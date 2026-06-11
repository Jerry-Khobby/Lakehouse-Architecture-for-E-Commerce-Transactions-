import random
from datetime import timedelta

from constants import TIMESTAMP_FMT, DATE_FMT


def random_timestamp(start, end):
    delta = int((end - start).total_seconds())
    return start + timedelta(seconds=random.randint(0, delta))


def format_timestamp(dt):
    return dt.strftime(TIMESTAMP_FMT)


def format_date(dt):
    return dt.strftime(DATE_FMT)
