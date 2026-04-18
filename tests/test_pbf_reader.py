"""Tests for PBF reader SQL safety."""

from __future__ import annotations

from ducknx import _pbf_reader


def test_tag_filter_escapes_single_quotes() -> None:
    """Test that tag filter construction escapes single quotes in values."""
    tags = {"name": "McDonald's"}
    conditions = _pbf_reader._build_tag_filter(tags)
    assert "''" in conditions
    assert "McDonald''s" in conditions


def test_tag_filter_bool_true() -> None:
    """Test tag filter with True value."""
    tags = {"building": True}
    conditions = _pbf_reader._build_tag_filter(tags)
    assert "tags['building'] IS NOT NULL" in conditions


def test_tag_filter_bool_false() -> None:
    """Test tag filter with False value."""
    tags = {"building": False}
    conditions = _pbf_reader._build_tag_filter(tags)
    assert "tags['building'] IS NULL" in conditions


def test_tag_filter_list_values() -> None:
    """Test tag filter with list of values."""
    tags = {"highway": ["primary", "O'Connell"]}
    conditions = _pbf_reader._build_tag_filter(tags)
    assert "O''Connell" in conditions


def test_tag_filter_empty() -> None:
    """Test tag filter with empty tags dict."""
    conditions = _pbf_reader._build_tag_filter({})
    assert conditions == "1=1"


def test_network_filter_all_and_all_public_identical() -> None:
    """Test that 'all' and 'all_public' produce identical SQL."""
    all_filter = _pbf_reader._get_network_filter_sql("all")
    all_public_filter = _pbf_reader._get_network_filter_sql("all_public")
    assert all_filter == all_public_filter
