"""
Geocode place names or addresses, or retrieve OSM elements by place name or ID.

This module uses the Nominatim "search" and "lookup" endpoints. For more
details see https://wiki.openstreetmap.org/wiki/Elements and
https://nominatim.org/.
"""

from __future__ import annotations

import logging as lg
from collections import OrderedDict
from typing import Any

import pyarrow as pa
import shapely

from . import _nominatim
from . import settings
from . import utils
from ._errors import InsufficientResponseError


def geocode(query: str) -> tuple[float, float]:
    """
    Geocode place names or addresses to ``(lat, lon)`` via the Nominatim API.

    Parameters
    ----------
    query
        The query string to geocode.

    Returns
    -------
    point
        The ``(lat, lon)`` coordinates returned by the geocoder.
    """
    params: OrderedDict[str, int | str] = OrderedDict()
    params["format"] = "json"
    params["limit"] = 1
    params["dedupe"] = 0  # prevent deduping to get precise number of results
    params["q"] = query
    response_json = _nominatim._nominatim_request(params=params)

    if response_json and "lat" in response_json[0] and "lon" in response_json[0]:
        lat = float(response_json[0]["lat"])
        lon = float(response_json[0]["lon"])
        point = (lat, lon)
        utils.log(f"Geocoded {query!r} to {point}", level=lg.INFO)
        return point

    msg = f"Nominatim could not geocode query {query!r}."
    raise InsufficientResponseError(msg)


def _wkb_geom_field(crs: str | None) -> pa.Field:
    """
    Build a `geometry` field carrying geoarrow.wkb extension metadata.

    Parameters
    ----------
    crs
        CRS authority code string or None.

    Returns
    -------
    field
        Arrow field tagged with geoarrow.wkb extension.
    """
    crs_token = (crs or "").upper() if crs else ""
    metadata = {
        b"ARROW:extension:name": b"geoarrow.wkb",
        b"ARROW:extension:metadata": (
            b'{"crs":"' + crs_token.encode("ascii") + b'","edges":"planar"}'
        ),
    }
    return pa.field("geometry", pa.binary(), nullable=True, metadata=metadata)


def geocode_to_arrow(
    query: str | dict[str, str] | list[str | dict[str, str]],
    *,
    which_result: int | None | list[int | None] = None,
    by_osmid: bool = False,
) -> pa.Table:
    """
    Retrieve OSM elements by place name or OSM ID via the Nominatim API.

    The returned table has a ``geometry`` column (geoarrow.wkb, CRS in
    field metadata) plus per-result columns: ``lat``, ``lon``,
    ``bbox_north``, ``bbox_south``, ``bbox_east``, ``bbox_west``, and any
    additional Nominatim metadata.

    Parameters
    ----------
    query
        Query string(s) or structured dict(s) to geocode.
    which_result
        Which search result to return. If None, auto-select the first
        (Multi)Polygon. Ignored if ``by_osmid=True``.
    by_osmid
        If True, treat query as an OSM ID lookup rather than text search.

    Returns
    -------
    table
        Arrow table with one row per query result.
    """
    if isinstance(query, list):
        q_list = query
        wr_list = which_result if isinstance(which_result, list) else [which_result] * len(query)
    else:
        q_list = [query]
        wr_list = [which_result[0]] if isinstance(which_result, list) else [which_result]

    if len(q_list) != len(wr_list):  # pragma: no cover
        msg = "`which_result` length must equal `query` length."
        raise ValueError(msg)

    rows = [_geocode_query_to_dict(q, wr, by_osmid)
            for q, wr in zip(q_list, wr_list, strict=True)]

    # collect a stable schema by union of all keys, geometry separated
    all_cols: list[str] = []
    for row in rows:
        for col in row:
            if col != "geometry" and col not in all_cols:
                all_cols.append(col)

    column_arrays: dict[str, list[Any]] = {col: [row.get(col) for row in rows] for col in all_cols}
    geom_arr = pa.array([shapely.to_wkb(row["geometry"]) for row in rows], type=pa.binary())

    table = pa.table(column_arrays)
    table = table.append_column(_wkb_geom_field(settings.default_crs), geom_arr)

    utils.log(f"Created Arrow table with {len(rows)} rows from {len(q_list)} queries",
              level=lg.INFO)
    return table


def _geocode_query_to_dict(
    query: str | dict[str, str],
    which_result: int | None,
    by_osmid: bool,  # noqa: FBT001
) -> dict[str, Any]:
    """
    Geocode a single place query into an attribute dict + shapely geometry.

    Parameters
    ----------
    query
        Query string or structured dict to geocode.
    which_result
        Which search result to return.
    by_osmid
        If True, treat query as an OSM ID lookup rather than text search.

    Returns
    -------
    row
        Dict with one ``geometry`` key (shapely geometry) plus scalar
        attribute fields.
    """
    limit = 50 if which_result is None else which_result
    results = _nominatim._download_nominatim_element(query, by_osmid=by_osmid, limit=limit)

    results = sorted(results, key=lambda x: x["importance"], reverse=True)

    if len(results) == 0:
        msg = f"Nominatim geocoder returned 0 results for query {query!r}."
        raise InsufficientResponseError(msg)

    if by_osmid:
        result = results[0]
    elif which_result is None:
        try:
            result = _get_first_polygon(results)
        except TypeError as e:
            msg = f"Nominatim did not geocode query {query!r} to a geometry of type (Multi)Polygon."
            raise TypeError(msg) from e
    elif len(results) >= which_result:
        result = results[which_result - 1]
    else:  # pragma: no cover
        msg = f"Nominatim returned {len(results)} result(s) but `which_result={which_result}`."
        raise InsufficientResponseError(msg)

    geom_type = result["geojson"]["type"]
    if geom_type not in {"Polygon", "MultiPolygon"}:
        utils.log(
            f"Nominatim geocoder returned a {geom_type} as the geometry for query {query!r}",
            level=lg.WARNING,
        )

    geom = shapely.from_geojson(_serialize_geojson(result["geojson"]))
    bottom, top, left, right = result["boundingbox"]
    row: dict[str, Any] = {
        "geometry": geom,
        "bbox_west": float(left),
        "bbox_south": float(bottom),
        "bbox_east": float(right),
        "bbox_north": float(top),
    }
    for attr, value in result.items():
        if attr in {"address", "boundingbox", "geojson", "icon", "licence"}:
            continue
        if attr in {"lat", "lon"}:
            row[attr] = float(value)
        else:
            row[attr] = value
    return row


def _serialize_geojson(geojson_obj: Any) -> str:  # noqa: ANN401
    """Encode a GeoJSON dict as a JSON string for ``shapely.from_geojson``."""
    import json  # noqa: PLC0415

    return json.dumps(geojson_obj)


def _get_first_polygon(results: list[dict[str, Any]]) -> dict[str, Any]:
    """
    Choose the first result of geometry type (Multi)Polygon.

    Parameters
    ----------
    results
        Results from the Nominatim API.

    Returns
    -------
    result
        The chosen result.
    """
    polygon_types = {"Polygon", "MultiPolygon"}
    for result in results:
        if "geojson" in result and result["geojson"]["type"] in polygon_types:
            return result
    raise TypeError
