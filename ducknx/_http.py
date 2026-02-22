"""Handle HTTP requests to web APIs."""

from __future__ import annotations

import json
import logging as lg
from hashlib import sha1
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import requests
from requests.exceptions import JSONDecodeError

from . import settings
from . import utils
from ._errors import InsufficientResponseError
from ._errors import ResponseStatusCodeError


def _save_to_cache(
    url: str,
    response_json: dict[str, Any] | list[dict[str, Any]],
    ok: bool,  # noqa: FBT001
) -> None:
    """
    Save a HTTP response JSON object to a file in the cache folder.

    If request was sent to server via POST instead of GET, then `url` should
    be a GET-style representation of the request. Response is only saved to a
    cache file if `settings.use_cache` is True, `ok` is True, `response_json`
    is not None, and `response_json` does not contain a server "remark."

    Users should always pass OrderedDicts instead of dicts of parameters into
    request functions, so the parameters remain in the same order each time,
    producing the same URL string, and thus the same hash. Otherwise you will
    get a cache miss when the URL's parameters appeared in a different order.

    Parameters
    ----------
    url
        The URL of the request.
    response_json
        The JSON HTTP response.
    ok
        A `requests.response.ok` value.
    """
    if settings.use_cache:
        if not ok:  # pragma: no cover
            msg = "Did not save to cache because HTTP status code is not OK"
            utils.log(msg, level=lg.WARNING)
        elif isinstance(response_json, dict) and ("remark" in response_json):  # pragma: no cover
            msg = f"Did not save to cache because response contains remark: {response_json['remark']!r}"
            utils.log(msg, lg.WARNING)
        else:
            # create cache folder on disk if it doesn't already exist
            cache_filepath = _resolve_cache_filepath(url)
            cache_filepath.parent.mkdir(parents=True, exist_ok=True)
            cache_filepath.write_text(json.dumps(response_json), encoding="utf-8")
            msg = f"Saved response to cache file {str(cache_filepath)!r}"
            utils.log(msg, level=lg.INFO)


def _resolve_cache_filepath(key: str, extension: str = "json") -> Path:
    """
    Determine a cache key's corresponding cache file path.

    This uses the configured `settings.cache_folder` and calculates the 160
    bit SHA-1 hash digest (40 hexadecimal characters) of `key` to determine a
    succinct but unique cache filename.

    Parameters
    ----------
    key
        The key for which to generate a cache file path, for example, a URL.
    extension
        The desired cache file's extension.

    Returns
    -------
    cache_filepath
        Cache file path corresponding to `key`.
    """
    digest = sha1(key.encode("utf-8")).hexdigest()  # noqa: S324
    return Path(settings.cache_folder) / f"{digest}.{extension}"


def _check_cache(key: str) -> Path | None:
    """
    Check if a key exists in the cache, and return its cache file path if so.

    Parameters
    ----------
    key
        The key to look for in the cache.

    Returns
    -------
    cache_filepath
        Filepath to cached data for `key` if it exists, otherwise None.
    """
    cache_filepath = _resolve_cache_filepath(key)
    return cache_filepath if cache_filepath.is_file() else None


def _retrieve_from_cache(url: str) -> dict[str, Any] | list[dict[str, Any]] | None:
    """
    Retrieve a HTTP response JSON object from the cache if it exists.

    A cache hit returns the data. A cache miss returns None.

    Parameters
    ----------
    url
        The URL of the request.

    Returns
    -------
    response_json
        The cached response for `url` if it exists, otherwise None.
    """
    # if the tool is configured to use the cache
    if settings.use_cache:
        # return cached response for this url if exists, otherwise return None
        cache_filepath = _check_cache(url)
        if cache_filepath is not None:
            response_json: dict[str, Any] | list[dict[str, Any]]
            response_json = json.loads(cache_filepath.read_text(encoding="utf-8"))
            msg = f"Retrieved response from cache file {str(cache_filepath)!r}"
            utils.log(msg, lg.INFO)
            return response_json

    return None


def _get_http_headers(
    *,
    user_agent: str | None = None,
    referer: str | None = None,
    accept_language: str | None = None,
) -> dict[str, str]:
    """
    Update the default requests HTTP headers with ducknx information.

    Parameters
    ----------
    user_agent
        The user agent. If None, use `settings.http_user_agent` value.
    referer
        The referer. If None, use `settings.http_referer` value.
    accept_language
        The accept language. If None, use `settings.http_accept_language`
        value.

    Returns
    -------
    headers
        The updated HTTP headers.
    """
    if user_agent is None:
        user_agent = settings.http_user_agent
    if referer is None:
        referer = settings.http_referer
    if accept_language is None:
        accept_language = settings.http_accept_language

    info = {"User-Agent": user_agent, "referer": referer, "Accept-Language": accept_language}
    headers = dict(requests.utils.default_headers())
    headers.update(info)
    return headers


def _hostname_from_url(url: str) -> str:
    """
    Extract the hostname (domain) from a URL.

    Parameters
    ----------
    url
        The url from which to extract the hostname.

    Returns
    -------
    hostname
        The extracted hostname (domain).
    """
    return urlparse(url).netloc.split(":")[0]


def _parse_response(response: requests.Response) -> dict[str, Any] | list[dict[str, Any]]:
    """
    Parse JSON from a requests response and log the details.

    Parameters
    ----------
    response
        The response object.

    Returns
    -------
    response_json
        Value will be a dict if the response is from the Google Elevation
        API, and a list if the response is from the Nominatim API.
    """
    # log the response size and hostname
    hostname = _hostname_from_url(response.url)
    size_kb = len(response.content) / 1000
    msg = f"Downloaded {size_kb:,.1f}kB from {hostname!r} with status {response.status_code}"
    utils.log(msg, level=lg.INFO)

    # parse the response to JSON and log/raise exceptions
    try:
        response_json: dict[str, Any] | list[dict[str, Any]] = response.json()
    except JSONDecodeError as e:  # pragma: no cover
        msg = f"{hostname!r} responded: {response.status_code} {response.reason} {response.text}"
        utils.log(msg, level=lg.ERROR)
        if response.ok:
            raise InsufficientResponseError(msg) from e
        raise ResponseStatusCodeError(msg) from e

    # log any remarks if they exist
    if isinstance(response_json, dict) and "remark" in response_json:  # pragma: no cover
        msg = f"{hostname!r} remarked: {response_json['remark']!r}"
        utils.log(msg, level=lg.WARNING)

    # log if the response status_code is not OK
    if not response.ok:
        msg = f"{hostname!r} returned HTTP status code {response.status_code}"
        utils.log(msg, level=lg.WARNING)

    return response_json
