from datetime import datetime

from helpers import format_date, format_timestamp, random_timestamp

MAY_START = datetime(2025, 5, 1, 0, 0, 0)
MAY_END = datetime(2025, 5, 31, 23, 59, 59)


def test_random_timestamp_returns_datetime_instance():
    assert isinstance(random_timestamp(MAY_START, MAY_END), datetime)


def test_random_timestamp_is_not_before_start():
    assert random_timestamp(MAY_START, MAY_END) >= MAY_START


def test_random_timestamp_is_not_after_end():
    assert random_timestamp(MAY_START, MAY_END) <= MAY_END


def test_random_timestamp_returns_start_when_range_is_zero():
    fixed = datetime(2025, 5, 15, 12, 0, 0)
    assert random_timestamp(fixed, fixed) == fixed


def test_format_timestamp_produces_expected_string():
    dt = datetime(2025, 5, 15, 10, 30, 45)
    assert format_timestamp(dt) == "2025-05-15 10:30:45"


def test_format_timestamp_pads_single_digit_values():
    dt = datetime(2025, 5, 1, 9, 5, 3)
    assert format_timestamp(dt) == "2025-05-01 09:05:03"


def test_format_date_returns_date_only_string():
    dt = datetime(2025, 5, 15, 10, 30, 45)
    assert format_date(dt) == "2025-05-15"


def test_format_date_strips_time_component():
    assert format_date(datetime(2025, 5, 15, 23, 59, 59)) == format_date(datetime(2025, 5, 15, 0, 0, 0))
