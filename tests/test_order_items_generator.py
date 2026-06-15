import csv
from collections import defaultdict

import pytest

import order_items_generator
from order_items_generator import (
    CLEAN_ITEM_TARGET,
    INVALID_PRODUCT_ID_BASE,
    generate_clean_items,
    generate_dirty_items,
    write_order_items,
)
from orders_generator import generate_clean_orders

DIRTY_ITEM_COUNT = 40


@pytest.fixture(scope="module")
def clean_orders():
    return generate_clean_orders()


@pytest.fixture(scope="module")
def valid_product_ids():
    return list(range(1, 1001))


@pytest.fixture(scope="module")
def clean_items_result(clean_orders, valid_product_ids):
    return generate_clean_items(clean_orders, valid_product_ids)


@pytest.fixture(scope="module")
def clean_items(clean_items_result):
    items, _ = clean_items_result
    return items


@pytest.fixture(scope="module")
def dirty_items(clean_items_result, clean_orders, valid_product_ids):
    _, next_id = clean_items_result
    return generate_dirty_items(next_id, clean_orders, valid_product_ids)


# ── generate_clean_items ───────────────────────────────────────────────────────


def test_generate_clean_items_reaches_target(clean_items):
    assert len(clean_items) == CLEAN_ITEM_TARGET


def test_generate_clean_items_next_id_is_target_plus_one(clean_items_result):
    items, next_id = clean_items_result
    assert next_id == len(items) + 1


def test_generate_clean_items_ids_are_sequential_from_one(clean_items):
    ids = [item["id"] for item in clean_items]
    assert ids == list(range(1, CLEAN_ITEM_TARGET + 1))


def test_generate_clean_items_order_ids_reference_clean_orders(clean_items, clean_orders):
    valid_order_ids = {o["order_id"] for o in clean_orders}
    for item in clean_items:
        assert item["order_id"] in valid_order_ids


def test_generate_clean_items_user_id_matches_order(clean_items, clean_orders):
    user_by_order = {o["order_id"]: o["user_id"] for o in clean_orders}
    for item in clean_items:
        assert item["user_id"] == user_by_order[item["order_id"]]


def test_generate_clean_items_product_ids_are_valid(clean_items, valid_product_ids):
    valid_set = set(valid_product_ids)
    for item in clean_items:
        assert item["product_id"] in valid_set


def test_generate_clean_items_reordered_is_binary(clean_items):
    for item in clean_items:
        assert item["reordered"] in (0, 1)


def test_generate_clean_items_add_to_cart_order_is_sequential_per_order(clean_items):
    cart_positions = defaultdict(list)
    for item in clean_items:
        cart_positions[item["order_id"]].append(item["add_to_cart_order"])
    for order_id, positions in cart_positions.items():
        assert sorted(positions) == list(range(1, len(positions) + 1))


def test_generate_clean_items_days_since_prior_is_empty_or_in_range(clean_items):
    for item in clean_items:
        val = item["days_since_prior_order"]
        if val != "":
            assert 0 <= int(val) <= 365


def test_generate_clean_items_each_order_has_at_most_8_items(clean_items):
    item_counts = defaultdict(int)
    for item in clean_items:
        item_counts[item["order_id"]] += 1
    for count in item_counts.values():
        assert count <= 8


def test_generate_clean_items_timestamps_match_order(clean_items, clean_orders):
    order_ts = {o["order_id"]: o["order_timestamp"] for o in clean_orders}
    for item in clean_items:
        assert item["order_timestamp"] == order_ts[item["order_id"]]


# ── generate_dirty_items ───────────────────────────────────────────────────────


def test_generate_dirty_items_returns_40_rows(dirty_items):
    assert len(dirty_items) == DIRTY_ITEM_COUNT


def test_generate_dirty_items_has_10_null_ids(dirty_items):
    assert sum(1 for r in dirty_items if r["id"] == "") == 10


def test_generate_dirty_items_null_id_rows_have_valid_order_ids(dirty_items, clean_orders):
    valid_order_ids = {o["order_id"] for o in clean_orders}
    for row in (r for r in dirty_items if r["id"] == ""):
        assert row["order_id"] in valid_order_ids


def test_generate_dirty_items_has_10_invalid_product_ids(dirty_items):
    assert sum(1 for r in dirty_items if int(r["product_id"]) >= INVALID_PRODUCT_ID_BASE) == 10


def test_generate_dirty_items_invalid_product_ids_are_out_of_range(dirty_items, valid_product_ids):
    valid_set = set(valid_product_ids)
    for row in (r for r in dirty_items if int(r["product_id"]) >= INVALID_PRODUCT_ID_BASE):
        assert row["product_id"] not in valid_set


def test_generate_dirty_items_has_10_ghost_order_ids(dirty_items):
    assert sum(1 for r in dirty_items if str(r["order_id"]).startswith("ghost_")) == 10


def test_generate_dirty_items_ghost_order_ids_follow_naming_pattern(dirty_items):
    ghost_ids = [r["order_id"] for r in dirty_items if str(r["order_id"]).startswith("ghost_")]
    assert sorted(ghost_ids) == [f"ghost_{i:03d}" for i in range(1, 11)]


def test_generate_dirty_items_has_10_invalid_reordered_flags(dirty_items):
    assert sum(1 for r in dirty_items if r["reordered"] == 5) == 10


def test_generate_dirty_items_four_categories_are_mutually_exclusive(dirty_items):
    null_id = sum(1 for r in dirty_items if r["id"] == "")
    invalid_pid = sum(1 for r in dirty_items if int(r["product_id"]) >= INVALID_PRODUCT_ID_BASE)
    ghost_oid = sum(1 for r in dirty_items if str(r["order_id"]).startswith("ghost_"))
    bad_reorder = sum(1 for r in dirty_items if r["reordered"] == 5)
    assert null_id + invalid_pid + ghost_oid + bad_reorder == DIRTY_ITEM_COUNT


# ── write_order_items ──────────────────────────────────────────────────────────

_STUB_CLEAN_ITEM = [
    {
        "id": 1,
        "order_id": "ord_11111",
        "user_id": "usr_001",
        "days_since_prior_order": "3",
        "product_id": 5,
        "add_to_cart_order": 1,
        "reordered": 0,
        "order_timestamp": "2025-05-01 10:00:00",
        "date": "2025-05-01",
    }
]
_STUB_DIRTY_ITEM = [
    {
        "id": "",
        "order_id": "ord_22222",
        "user_id": "usr_002",
        "days_since_prior_order": "7",
        "product_id": 10,
        "add_to_cart_order": 1,
        "reordered": 1,
        "order_timestamp": "2025-05-02 11:00:00",
        "date": "2025-05-02",
    }
]


def test_write_order_items_creates_file(tmp_path, monkeypatch):
    monkeypatch.setattr(order_items_generator, "OUTPUT_DIR", str(tmp_path))
    write_order_items(_STUB_CLEAN_ITEM, _STUB_DIRTY_ITEM)
    assert (tmp_path / "order_items_may_2025.csv").exists()


def test_write_order_items_header_is_correct(tmp_path, monkeypatch):
    monkeypatch.setattr(order_items_generator, "OUTPUT_DIR", str(tmp_path))
    write_order_items([], [])
    with open(tmp_path / "order_items_may_2025.csv", newline="", encoding="utf-8") as f:
        header = next(csv.reader(f))
    assert header == [
        "id",
        "order_id",
        "user_id",
        "days_since_prior_order",
        "product_id",
        "add_to_cart_order",
        "reordered",
        "order_timestamp",
        "date",
    ]


def test_write_order_items_row_count_equals_clean_plus_dirty(tmp_path, monkeypatch):
    monkeypatch.setattr(order_items_generator, "OUTPUT_DIR", str(tmp_path))
    write_order_items(_STUB_CLEAN_ITEM, _STUB_DIRTY_ITEM)
    with open(tmp_path / "order_items_may_2025.csv", newline="", encoding="utf-8") as f:
        rows = list(csv.reader(f))
    assert len(rows) == 3  # 1 header + 2 data rows


def test_write_order_items_omits_extra_fields(tmp_path, monkeypatch):
    monkeypatch.setattr(order_items_generator, "OUTPUT_DIR", str(tmp_path))
    row_with_extra = {**_STUB_CLEAN_ITEM[0], "extra_field": "should_be_stripped"}
    write_order_items([row_with_extra], [])
    with open(tmp_path / "order_items_may_2025.csv", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            assert "extra_field" not in row
