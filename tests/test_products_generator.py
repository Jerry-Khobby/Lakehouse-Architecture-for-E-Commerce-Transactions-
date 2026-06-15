import csv

import pytest

import products_generator
from constants import DEPARTMENTS
from products_generator import generate_products, write_products

DEPT_ID_TO_NAME = {dept_id: dept_name for dept_id, dept_name in DEPARTMENTS}
KNOWN_DEPT_NAMES = {dept_name for _, dept_name in DEPARTMENTS}


@pytest.fixture(scope="module")
def products():
    return generate_products()


def test_generate_products_returns_1000_rows(products):
    assert len(products) == 1000


def test_generate_products_ids_are_sequential_from_one(products):
    ids = [p[0] for p in products]
    assert ids == list(range(1, 1001))


def test_generate_products_ids_are_unique(products):
    ids = [p[0] for p in products]
    assert len(ids) == len(set(ids))


def test_generate_products_each_row_has_four_fields(products):
    for product in products:
        assert len(product) == 4


def test_generate_products_department_id_maps_consistently_to_name(products):
    for _, dept_id, dept_name, _ in products:
        assert DEPT_ID_TO_NAME.get(dept_id) == dept_name


def test_generate_products_uses_only_known_departments(products):
    for _, _, dept_name, _ in products:
        assert dept_name in KNOWN_DEPT_NAMES


def test_generate_products_all_product_names_are_non_empty(products):
    for _, _, _, product_name in products:
        assert product_name.strip() != ""


def test_generate_products_all_departments_appear_at_least_once(products):
    used_dept_names = {dept_name for _, _, dept_name, _ in products}
    assert used_dept_names == KNOWN_DEPT_NAMES


def test_write_products_creates_file(tmp_path, monkeypatch):
    monkeypatch.setattr(products_generator, "OUTPUT_DIR", str(tmp_path))
    write_products([(1, 1, "produce", "Organic Bananas")])
    assert (tmp_path / "products.csv").exists()


def test_write_products_header_row_is_correct(tmp_path, monkeypatch):
    monkeypatch.setattr(products_generator, "OUTPUT_DIR", str(tmp_path))
    write_products([])
    with open(tmp_path / "products.csv", newline="", encoding="utf-8") as f:
        header = next(csv.reader(f))
    assert header == ["product_id", "department_id", "department", "product_name"]


def test_write_products_writes_all_rows(tmp_path, monkeypatch, products):
    monkeypatch.setattr(products_generator, "OUTPUT_DIR", str(tmp_path))
    write_products(products)
    with open(tmp_path / "products.csv", newline="", encoding="utf-8") as f:
        rows = list(csv.reader(f))
    assert len(rows) == 1001  # 1 header + 1000 data rows


def test_write_products_row_values_match_input(tmp_path, monkeypatch):
    monkeypatch.setattr(products_generator, "OUTPUT_DIR", str(tmp_path))
    write_products([(42, 3, "snacks", "Kettle Chips")])
    with open(tmp_path / "products.csv", newline="", encoding="utf-8") as f:
        reader = csv.reader(f)
        next(reader)
        row = next(reader)
    assert row == ["42", "3", "snacks", "Kettle Chips"]
