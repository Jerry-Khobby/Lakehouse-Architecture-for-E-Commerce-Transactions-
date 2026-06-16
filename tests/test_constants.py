from datetime import datetime

import constants


def test_departments_count_is_ten():
    assert len(constants.DEPARTMENTS) == 10


def test_department_ids_are_unique():
    ids = [dept_id for dept_id, _ in constants.DEPARTMENTS]
    assert len(ids) == len(set(ids))


def test_department_ids_are_sequential_from_one():
    ids = sorted(dept_id for dept_id, _ in constants.DEPARTMENTS)
    assert ids == list(range(1, 11))


def test_product_names_has_entry_for_every_department():
    for _, dept_name in constants.DEPARTMENTS:
        assert dept_name in constants.PRODUCT_NAMES


def test_each_department_name_pool_is_non_empty():
    for _, dept_name in constants.DEPARTMENTS:
        assert len(constants.PRODUCT_NAMES[dept_name]) > 0


def test_may_start_is_first_second_of_may_2025():
    assert constants.MAY_START == datetime(2025, 5, 1, 0, 0, 0)


def test_may_end_is_last_second_of_may_2025():
    assert constants.MAY_END == datetime(2025, 5, 31, 23, 59, 59)


def test_future_start_is_beginning_of_2027():
    assert constants.FUTURE_START == datetime(2027, 1, 1, 0, 0, 0)


def test_future_end_is_end_of_2027():
    assert constants.FUTURE_END == datetime(2027, 12, 31, 23, 59, 59)


def test_future_range_end_is_after_start():
    assert constants.FUTURE_END > constants.FUTURE_START


def test_timestamp_fmt_renders_full_datetime():
    dt = datetime(2025, 5, 15, 10, 30, 45)
    assert dt.strftime(constants.TIMESTAMP_FMT) == "2025-05-15T10:30:45"


def test_date_fmt_renders_date_only():
    dt = datetime(2025, 5, 15, 10, 30, 45)
    assert dt.strftime(constants.DATE_FMT) == "2025-05-15"
