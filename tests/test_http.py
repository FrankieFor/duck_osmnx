"""Tests for HTTP utilities."""

from __future__ import annotations

import httpx
import pytest

from ducknx import _http


def test_parse_response_valid_json() -> None:
    """Test _parse_response with a valid JSON response."""
    response = httpx.Response(
        status_code=200,
        json={"results": [{"elevation": 10.0}]},
        request=httpx.Request("GET", "https://example.com/api"),
    )
    result = _http._parse_response(response)
    assert isinstance(result, dict)
    assert "results" in result


def test_parse_response_list_json() -> None:
    """Test _parse_response with a list JSON response (like Nominatim)."""
    response = httpx.Response(
        status_code=200,
        json=[{"place_id": 1, "display_name": "test"}],
        request=httpx.Request("GET", "https://nominatim.example.com/search"),
    )
    result = _http._parse_response(response)
    assert isinstance(result, list)
    assert len(result) == 1


def test_parse_response_not_ok_logs_warning() -> None:
    """Test _parse_response logs warning for non-OK status."""
    response = httpx.Response(
        status_code=400,
        json={"error": "bad request"},
        request=httpx.Request("GET", "https://example.com/api"),
    )
    # Should not raise, just log warning and return the json
    result = _http._parse_response(response)
    assert isinstance(result, dict)


def test_get_http_headers() -> None:
    """Test that _get_http_headers returns proper headers."""
    headers = _http._get_http_headers()
    assert "User-Agent" in headers
    assert "referer" in headers
    assert "Accept-Language" in headers


def test_hostname_from_url() -> None:
    """Test extracting hostname from URL."""
    assert _http._hostname_from_url("https://example.com/path") == "example.com"
    assert _http._hostname_from_url("https://api.example.com:8080/path") == "api.example.com"
