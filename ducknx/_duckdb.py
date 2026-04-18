"""DuckDB connection lifecycle manager for local PBF file queries."""

from __future__ import annotations

import logging as lg
from pathlib import Path

import duckdb

from . import utils

_connection: duckdb.DuckDBPyConnection | None = None
_current_pbf_path: Path | None = None


def _escape_sql(value: str) -> str:
    """
    Escape single quotes in a string for safe SQL interpolation.

    Parameters
    ----------
    value
        The string value to escape.

    Returns
    -------
    escaped
        The escaped string with single quotes doubled.
    """
    return value.replace("'", "''")


def get_connection(pbf_path: str | Path) -> duckdb.DuckDBPyConnection:
    """
    Return a DuckDB connection with spatial extension loaded and PBF data available.

    On first call, creates a connection, installs/loads the spatial extension,
    and loads the PBF file into a persistent ``osm_data`` temp table. On
    subsequent calls with the same path, returns the cached connection. If the
    path changes, closes the old connection and creates a new one.

    Parameters
    ----------
    pbf_path
        Path to the local OSM PBF file.

    Returns
    -------
    conn
        A DuckDB connection with the ``osm_data`` table ready to query.

    Raises
    ------
    FileNotFoundError
        If the PBF file does not exist.
    """
    global _connection, _current_pbf_path  # noqa: PLW0603

    pbf_path = Path(pbf_path)
    if not pbf_path.exists():
        msg = f"PBF file not found: {pbf_path}"
        raise FileNotFoundError(msg)

    # return cached connection if same path
    if _connection is not None and _current_pbf_path == pbf_path:
        return _connection

    # close existing connection if path changed
    if _connection is not None:
        close()

    conn = duckdb.connect()
    conn.execute("INSTALL spatial")
    conn.execute("LOAD spatial")

    escaped_path = _escape_sql(str(pbf_path))
    conn.execute(f"CREATE TEMP TABLE osm_data AS SELECT * FROM ST_ReadOSM('{escaped_path}')")

    msg = f"Loaded PBF file into DuckDB: {pbf_path}"
    utils.log(msg, level=lg.INFO)

    _connection = conn
    _current_pbf_path = pbf_path
    return conn


def close() -> None:
    """
    Close the cached DuckDB connection and reset state.

    Safe to call multiple times. Does nothing if no connection is open.
    """
    global _connection, _current_pbf_path  # noqa: PLW0603
    if _connection is not None:
        try:
            _connection.close()
        except Exception:  # noqa: BLE001
            pass
    _connection = None
    _current_pbf_path = None
