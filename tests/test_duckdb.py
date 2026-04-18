"""Tests for the DuckDB connection manager."""

from __future__ import annotations

from pathlib import Path

import pytest

from ducknx import _duckdb


@pytest.fixture(autouse=True)
def _reset_connection():
    """Reset the connection manager state before and after each test."""
    _duckdb.close()
    yield
    _duckdb.close()


def test_get_connection_returns_duckdb_connection(tmp_path: Path) -> None:
    """Test that get_connection raises for a non-existent PBF file."""
    with pytest.raises(FileNotFoundError):
        _duckdb.get_connection(tmp_path / "nonexistent.pbf")


def test_close_resets_state() -> None:
    """Test that close() resets internal state."""
    _duckdb.close()
    assert _duckdb._connection is None
    assert _duckdb._current_pbf_path is None


def test_close_idempotent() -> None:
    """Test that calling close() multiple times is safe."""
    _duckdb.close()
    _duckdb.close()
    assert _duckdb._connection is None


def test_escape_sql_basic() -> None:
    """Test SQL escaping of single quotes."""
    assert _duckdb._escape_sql("McDonald's") == "McDonald''s"
    assert _duckdb._escape_sql("no quotes") == "no quotes"
    assert _duckdb._escape_sql("it''s") == "it''''s"
    assert _duckdb._escape_sql("") == ""


def test_escape_sql_multiple_quotes() -> None:
    """Test SQL escaping with multiple single quotes."""
    assert _duckdb._escape_sql("a'b'c") == "a''b''c"
