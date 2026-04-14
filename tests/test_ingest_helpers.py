"""Unit tests for pure helpers in scripts/ingest_tlc_2019_2020.py (no network, no GCP)."""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
INGEST_PATH = ROOT / "scripts" / "ingest_tlc_2019_2020.py"


@pytest.fixture(scope="module")
def ingest_mod():
    spec = importlib.util.spec_from_file_location("ingest_tlc", INGEST_PATH)
    assert spec and spec.loader
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def test_ym_list_single_month(ingest_mod):
    assert ingest_mod.ym_list(2020, 12, 2020, 12) == ["2020-12"]


def test_ym_list_full_year_2019(ingest_mod):
    assert ingest_mod.ym_list(2019, 1, 2019, 12) == [f"2019-{m:02d}" for m in range(1, 13)]


def test_ym_list_spans_year_boundary(ingest_mod):
    assert ingest_mod.ym_list(2019, 11, 2020, 2) == [
        "2019-11",
        "2019-12",
        "2020-01",
        "2020-02",
    ]


def test_ym_list_empty_when_range_invalid(ingest_mod):
    assert ingest_mod.ym_list(2020, 1, 2019, 12) == []
