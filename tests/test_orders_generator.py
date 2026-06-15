import csv
from datetime import datetime

import pytest

import orders_generator
from constants import DATE_FMT, MAY_END, MAY_START, TIMESTAMP_FMT
from orders_generator import (
    CLEAN_ORDER_COUNT,
    USER_POOL,
    generate_clean_orders,
    generate_dirty_orders,
    write_orders,
)

USER_POOL_SET = set(USER_POOL)
DIRTY_ORDER_COUNT = 50


@pytest.fixture(scope="module")
def clean_orders():
    return generate_clean_orders()


@pytest.fixture(scope="module")
def dirty_orders(clean_orders):
    return generate_dirty_orders(clean_orders)


# ── generate_clean_orders ──────────────────────────────────────────────────────

def test_generate_clean_orders_returns_800_rows(clean_orders):
    assert len(clean_orders) == CLEAN_ORDER_COUNT


def test_generate_clean_orders_order_nums_are_sequential_from_one(clean_orders):
    assert [o["order_num"] for o in clean_orders] == list(range(1, CLEAN_ORDER_COUNT + 1))


def test_generate_clean_orders_order_ids_are_unique(clean_orders):
    ids = [o["order_id"] for o in clean_orders]
    assert len(ids) == len(set(ids))


def test_generate_clean_orders_order_ids_have_ord_prefix(clean_orders):
    for order in clean_orders:
        assert order["order_id"].startswith("ord_")


def test_generate_clean_orders_timestamps_are_within_may_2025(clean_orders):
    for order in clean_orders:
        ts = datetime.strptime(order["order_timestamp"], TIMESTAMP_FMT)
        assert MAY_START <= ts <= MAY_END


def test_generate_clean_orders_amounts_are_within_valid_range(clean_orders):
    for order in clean_orders:
        amount = float(order["total_amount"])
        assert 5.00 <= amount <= 500.00


def test_generate_clean_orders_date_always_matches_timestamp(clean_orders):
    for order in clean_orders:
        ts = datetime.strptime(order["order_timestamp"], TIMESTAMP_FMT)
        assert order["date"] == ts.strftime(DATE_FMT)


def test_generate_clean_orders_user_ids_are_from_valid_pool(clean_orders):
    for order in clean_orders:
        assert order["user_id"] in USER_POOL_SET


# ── generate_dirty_orders ──────────────────────────────────────────────────────

def test_generate_dirty_orders_returns_50_rows(dirty_orders):
    assert len(dirty_orders) == DIRTY_ORDER_COUNT


def test_generate_dirty_orders_has_10_null_order_ids(dirty_orders):
    assert sum(1 for r in dirty_orders if r["order_id"] == "") == 10


def test_generate_dirty_orders_null_order_ids_are_empty_strings(dirty_orders):
    for row in (r for r in dirty_orders if r["order_id"] == ""):
        assert row["order_id"] == ""


def test_generate_dirty_orders_has_10_negative_amounts(dirty_orders):
    assert sum(1 for r in dirty_orders if float(r["total_amount"]) < 0) == 10


def test_generate_dirty_orders_negative_amounts_are_below_zero(dirty_orders):
    for row in (r for r in dirty_orders if float(r["total_amount"]) < 0):
        assert float(row["total_amount"]) < 0


def test_generate_dirty_orders_has_10_future_timestamps(dirty_orders):
    assert sum(1 for r in dirty_orders if r["order_timestamp"].startswith("2027-")) == 10


def test_generate_dirty_orders_future_timestamps_are_in_2027(dirty_orders):
    for row in (r for r in dirty_orders if r["order_timestamp"].startswith("2027-")):
        ts = datetime.strptime(row["order_timestamp"], TIMESTAMP_FMT)
        assert ts.year == 2027


def test_generate_dirty_orders_has_10_date_mismatches(dirty_orders):
    mismatch_count = sum(
        1 for r in dirty_orders
        if r["date"] != r["order_timestamp"][:10]
    )
    assert mismatch_count == 10


def test_generate_dirty_orders_date_mismatch_rows_have_shifted_date(dirty_orders):
    for row in (r for r in dirty_orders if r["date"] != r["order_timestamp"][:10]):
        ts_date = row["order_timestamp"][:10]
        assert row["date"] > ts_date


def test_generate_dirty_orders_has_10_duplicate_order_ids(dirty_orders, clean_orders):
    clean_ids = {o["order_id"] for o in clean_orders}
    # duplicates are the rows that are not null, not negative, not future, not date-mismatch
    duplicates = [
        r for r in dirty_orders
        if r["order_id"] != ""
        and float(r["total_amount"]) >= 0
        and not r["order_timestamp"].startswith("2027-")
        and r["date"] == r["order_timestamp"][:10]
    ]
    assert len(duplicates) == 10
    for row in duplicates:
        assert row["order_id"] in clean_ids


def test_generate_dirty_orders_duplicate_amounts_differ_from_original(dirty_orders, clean_orders):
    clean_by_id = {o["order_id"]: float(o["total_amount"]) for o in clean_orders}
    duplicates = [
        r for r in dirty_orders
        if r["order_id"] != ""
        and float(r["total_amount"]) >= 0
        and not r["order_timestamp"].startswith("2027-")
        and r["date"] == r["order_timestamp"][:10]
    ]
    for row in duplicates:
        assert float(row["total_amount"]) != clean_by_id[row["order_id"]]


# ── write_orders ───────────────────────────────────────────────────────────────

_STUB_CLEAN = [
    {
        "order_num": 1, "order_id": "ord_11111", "user_id": "usr_001",
        "order_timestamp": "2025-05-01 10:00:00", "total_amount": "99.99", "date": "2025-05-01",
    }
]
_STUB_DIRTY = [
    {
        "order_num": 801, "order_id": "", "user_id": "usr_002",
        "order_timestamp": "2025-05-02 11:00:00", "total_amount": "50.00", "date": "2025-05-02",
    }
]


def test_write_orders_creates_file(tmp_path, monkeypatch):
    monkeypatch.setattr(orders_generator, "OUTPUT_DIR", str(tmp_path))
    write_orders(_STUB_CLEAN, _STUB_DIRTY)
    assert (tmp_path / "orders_may_2025.csv").exists()


def test_write_orders_header_is_correct(tmp_path, monkeypatch):
    monkeypatch.setattr(orders_generator, "OUTPUT_DIR", str(tmp_path))
    write_orders([], [])
    with open(tmp_path / "orders_may_2025.csv", newline="", encoding="utf-8") as f:
        header = next(csv.reader(f))
    assert header == ["order_num", "order_id", "user_id", "order_timestamp", "total_amount", "date"]


def test_write_orders_row_count_equals_clean_plus_dirty(tmp_path, monkeypatch):
    monkeypatch.setattr(orders_generator, "OUTPUT_DIR", str(tmp_path))
    write_orders(_STUB_CLEAN, _STUB_DIRTY)
    with open(tmp_path / "orders_may_2025.csv", newline="", encoding="utf-8") as f:
        rows = list(csv.reader(f))
    assert len(rows) == 3  # 1 header + 2 data rows


def test_write_orders_omits_extra_fields(tmp_path, monkeypatch):
    monkeypatch.setattr(orders_generator, "OUTPUT_DIR", str(tmp_path))
    row_with_extra = {**_STUB_CLEAN[0], "extra_field": "should_be_stripped"}
    write_orders([row_with_extra], [])
    with open(tmp_path / "orders_may_2025.csv", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            assert "extra_field" not in row
