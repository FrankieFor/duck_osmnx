"""Tools to work with local OSM PBF files using DuckDB."""

from __future__ import annotations

import logging as lg
from pathlib import Path

import pyarrow as pa
from shapely import MultiPolygon
from shapely import Polygon

from . import _duckdb
from . import settings
from . import utils
from ._errors import InsufficientResponseError


def _get_network_filter_sql(network_type: str) -> str:
    """
    Create a SQL WHERE clause to filter ways for the specified network type.
    
    This function converts ducknx network type filters to SQL conditions
    that can be used with DuckDB's ST_ReadOSM function.
    
    Parameters
    ----------
    network_type
        {"all", "all_public", "bike", "drive", "drive_service", "walk"}
        What type of street network to retrieve.
        
    Returns
    -------
    sql_filter
        The SQL WHERE clause for filtering ways.
    """
    # Base condition: must have highway tag and not be an area
    base_conditions = [
        "tags['highway'] IS NOT NULL",
        "COALESCE(tags['area'], '') != 'yes'"
    ]
    
    # Access filter - properly handle NULL access tags (most ways have no access tag)
    # Only exclude ways that explicitly have access=private
    base_conditions.append("COALESCE(tags['access'], '') != 'private'")
    
    if network_type == "all":
        # All public and private-access ways currently in use
        return " AND ".join(base_conditions)
        
    elif network_type == "all_public":
        # All public ways currently in use (same as 'all' since we already exclude private)
        return " AND ".join(base_conditions)
        
    elif network_type == "drive":
        # Public drivable streets excluding service roads, private ways, motor=no
        drive_conditions = base_conditions + [
            "tags['highway'] NOT IN ('abandoned', 'bridleway', 'bus_guideway', 'construction', 'corridor', 'cycleway', 'elevator', 'escalator', 'footway', 'no', 'path', 'pedestrian', 'planned', 'platform', 'proposed', 'raceway', 'razed', 'service', 'steps', 'track')",
            "COALESCE(tags['motor_vehicle'], '') != 'no'",
            "COALESCE(tags['motorcar'], '') != 'no'",
            "COALESCE(tags['service'], '') NOT IN ('alley', 'driveway', 'emergency_access', 'parking', 'parking_aisle', 'private')"
        ]
        return " AND ".join(drive_conditions)
        
    elif network_type == "drive_service":
        # Drivable streets including service roads but excluding certain services
        drive_service_conditions = base_conditions + [
            "tags['highway'] NOT IN ('abandoned', 'bridleway', 'bus_guideway', 'construction', 'corridor', 'cycleway', 'elevator', 'escalator', 'footway', 'no', 'path', 'pedestrian', 'planned', 'platform', 'proposed', 'raceway', 'razed', 'steps', 'track')",
            "COALESCE(tags['motor_vehicle'], '') != 'no'",
            "COALESCE(tags['motorcar'], '') != 'no'",
            "COALESCE(tags['service'], '') NOT IN ('emergency_access', 'parking', 'parking_aisle', 'private')"
        ]
        return " AND ".join(drive_service_conditions)
        
    elif network_type == "bike":
        # Public bikeable ways excluding foot ways, motor ways, biking=no
        bike_conditions = base_conditions + [
            "tags['highway'] NOT IN ('abandoned', 'bus_guideway', 'construction', 'corridor', 'elevator', 'escalator', 'footway', 'motor', 'motorway', 'motorway_link', 'no', 'planned', 'platform', 'proposed', 'raceway', 'razed', 'steps')",
            "COALESCE(tags['bicycle'], '') != 'no'",
            "COALESCE(tags['biking'], '') != 'no'"
        ]
        return " AND ".join(bike_conditions)
        
    elif network_type == "walk":
        # Public walkable ways excluding cycle ways, motor ways, foot=no
        walk_conditions = base_conditions + [
            "tags['highway'] NOT IN ('abandoned', 'bus_guideway', 'construction', 'corridor', 'cycleway', 'elevator', 'escalator', 'motor', 'motorway', 'motorway_link', 'no', 'planned', 'platform', 'proposed', 'raceway', 'razed')",
            "COALESCE(tags['foot'], '') != 'no'"
        ]
        return " AND ".join(walk_conditions)
        
    else:
        msg = f"Unknown network_type: {network_type}"
        raise ValueError(msg)


def _build_tag_filter(tags: dict[str, bool | str | list[str]]) -> str:
    """
    Build a SQL WHERE clause to filter OSM elements by tags.

    Escapes all string values to prevent SQL injection from values
    containing single quotes.

    Parameters
    ----------
    tags
        Tags used for finding elements. Keys are OSM tag names and values
        can be True (tag exists), False (tag missing), a string (exact
        match), or a list of strings (any match).

    Returns
    -------
    tag_filter
        SQL WHERE clause string.
    """
    tag_conditions = []
    for key, value in tags.items():
        escaped_key = _duckdb._escape_sql(key)
        if isinstance(value, bool):
            if value:
                tag_conditions.append(f"tags['{escaped_key}'] IS NOT NULL")
            else:
                tag_conditions.append(f"tags['{escaped_key}'] IS NULL")
        elif isinstance(value, str):
            escaped_value = _duckdb._escape_sql(value)
            tag_conditions.append(f"tags['{escaped_key}'] = '{escaped_value}'")
        elif isinstance(value, list):
            escaped_values = "', '".join(_duckdb._escape_sql(v) for v in value)
            tag_conditions.append(f"tags['{escaped_key}'] IN ('{escaped_values}')")

    return " OR ".join(tag_conditions) if tag_conditions else "1=1"


def _build_polygon_case_sql() -> str:
    """
    Build a SQL CASE expression implementing OSM polygon classification rules.

    Translates the _POLYGON_FEATURES dict from features.py into a SQL CASE
    expression that determines whether a closed way should be a Polygon.

    Returns
    -------
    case_sql
        SQL CASE expression returning TRUE/FALSE.
    """
    # Import inside function to avoid circular imports
    from .features import _POLYGON_FEATURES  # noqa: PLC0415

    conditions = []
    # area=no always means not a polygon
    conditions.append("WHEN tags['area'] = 'no' THEN FALSE")

    for tag, rule_dict in _POLYGON_FEATURES.items():
        escaped_tag = _duckdb._escape_sql(tag)
        rule = rule_dict["polygon"]
        if rule == "all":
            conditions.append(f"WHEN tags['{escaped_tag}'] IS NOT NULL THEN TRUE")
        elif rule == "passlist":
            values = rule_dict.get("values", set())
            if values:
                escaped_vals = "', '".join(_duckdb._escape_sql(v) for v in sorted(values))
                conditions.append(
                    f"WHEN tags['{escaped_tag}'] IN ('{escaped_vals}') THEN TRUE",
                )
        elif rule == "blocklist":
            values = rule_dict.get("values", set())
            if values:
                escaped_vals = "', '".join(_duckdb._escape_sql(v) for v in sorted(values))
                conditions.append(
                    f"WHEN tags['{escaped_tag}'] IS NOT NULL "
                    f"AND tags['{escaped_tag}'] NOT IN ('{escaped_vals}') THEN TRUE",
                )
            else:
                conditions.append(f"WHEN tags['{escaped_tag}'] IS NOT NULL THEN TRUE")

    case_lines = "\n            ".join(conditions)
    return f"""CASE
            {case_lines}
            ELSE FALSE
        END"""


def _read_pbf_network_duckdb(
    polygon: Polygon | MultiPolygon,
    network_type: str,
    custom_filter: str | list[str] | None,
    pbf_path: str | Path,
) -> tuple[pa.Table, pa.Table]:
    """
    Retrieve networked ways and nodes from a local PBF file using DuckDB SQL.

    This optimized code path performs filtering and joins in DuckDB and returns
    Arrow tables directly, avoiding intermediate JSON dict conversion.

    Parameters
    ----------
    polygon
        The geometry within which to retrieve data.
    network_type
        What type of street network to retrieve.
    custom_filter
        Additional custom filter conditions as SQL WHERE clauses.
    pbf_path
        Path to the local OSM PBF file.

    Returns
    -------
    nodes_df, ways_df
        Arrow tables of nodes and ways.

    Raises
    ------
    InsufficientResponseError
        If no data was retrieved from the PBF file.
    """
    conn = _duckdb.get_connection(pbf_path)

    polygon_wkt = _duckdb._escape_sql(polygon.wkt)
    network_filter = _get_network_filter_sql(network_type)

    if custom_filter:
        if isinstance(custom_filter, list):
            custom_filter = " AND ".join(custom_filter)
        network_filter += f" AND ({custom_filter})"

    # Drop temp tables from any previous call
    conn.execute("DROP TABLE IF EXISTS area_nodes")
    conn.execute("DROP TABLE IF EXISTS filtered_ways")

    # Find nodes in the spatial area
    conn.execute(f"""
        CREATE TEMP TABLE area_nodes AS
        SELECT id FROM osm_data
        WHERE kind = 'node'
        AND ST_Within(ST_Point(lon, lat), ST_GeomFromText('{polygon_wkt}'))
    """)

    # Find ways matching filter that have at least one node in spatial area
    conn.execute(f"""
        CREATE TEMP TABLE filtered_ways AS
        SELECT id, tags, refs
        FROM osm_data
        WHERE kind = 'way'
        AND {network_filter}
        AND refs IS NOT NULL AND array_length(refs) > 0
        AND EXISTS (
            SELECT 1 FROM UNNEST(refs) AS t(node_id)
            WHERE node_id IN (SELECT id FROM area_nodes)
        )
    """)

    # Build ways SELECT with useful tags extracted in SQL
    way_tag_cols = ", ".join(
        f"tags['{_duckdb._escape_sql(tag)}'] AS \"{tag}\"" for tag in settings.useful_tags_way
    )
    ways_df = conn.execute(f"""
        SELECT
            id AS osmid,
            refs,
            {way_tag_cols}
        FROM filtered_ways
    """).fetch_arrow_table()

    if len(ways_df) == 0:
        msg = f"No ways found matching network filter in {pbf_path}"
        raise InsufficientResponseError(msg)

    utils.log(f"Found {len(ways_df)} ways matching network filter", level=lg.INFO)

    # Get ALL nodes referenced by filtered ways (not just in area)
    node_tag_cols = ", ".join(
        f"n.tags['{_duckdb._escape_sql(tag)}'] AS \"{tag}\"" for tag in settings.useful_tags_node
    )
    nodes_df = conn.execute(f"""
        SELECT
            n.id,
            n.lat AS y,
            n.lon AS x,
            {node_tag_cols}
        FROM osm_data n
        INNER JOIN (
            SELECT DISTINCT UNNEST(refs) AS node_id FROM filtered_ways
        ) r ON n.id = r.node_id
        WHERE n.kind = 'node'
    """).fetch_arrow_table()

    if len(nodes_df) == 0:
        msg = "No nodes found for the filtered ways"
        raise InsufficientResponseError(msg)

    utils.log(f"Found {len(nodes_df)} nodes for filtered ways", level=lg.INFO)

    return nodes_df, ways_df


def _read_pbf_features_duckdb(
    polygon: Polygon | MultiPolygon,
    tags: dict[str, bool | str | list[str]],
    pbf_path: str | Path,
) -> tuple[pa.Table, pa.Table, pa.Table]:
    """
    Retrieve OSM features from a local PBF file using DuckDB SQL.

    This optimized code path performs filtering and geometry construction in
    DuckDB and returns Arrow tables directly.

    Parameters
    ----------
    polygon
        The geometry within which to retrieve features.
    tags
        Tags used for finding elements in the search area.
    pbf_path
        Path to the local OSM PBF file.

    Returns
    -------
    nodes_df, ways_df, relations_df
        Arrow tables of nodes, ways, and relations.

    Raises
    ------
    InsufficientResponseError
        If no data was retrieved from the PBF file.
    """
    conn = _duckdb.get_connection(pbf_path)

    polygon_wkt = _duckdb._escape_sql(polygon.wkt)
    tag_filter = _build_tag_filter(tags)

    # Drop temp tables from any previous call
    for table in [
        "feature_area_nodes", "tagged_ways", "matching_ways_with_node_refs",
        "required_nodes_with_geometries", "matching_ways_linestrings",
        "matching_relations", "matching_relations_with_ways_refs",
        "required_ways_linestrings", "matching_relations_with_ways_linestrings",
        "matching_relations_with_merged_polygons", "outer_with_holes",
        "outer_without_holes",
    ]:
        conn.execute(f"DROP TABLE IF EXISTS {table}")

    # Query nodes with geometry
    nodes_df = conn.execute(f"""
        SELECT
            id,
            tags,
            ST_AsWKB(ST_Point(lon, lat)) AS geometry
        FROM osm_data
        WHERE kind = 'node'
        AND ({tag_filter})
        AND ST_Within(ST_Point(lon, lat), ST_GeomFromText('{polygon_wkt}'))
    """).fetch_arrow_table()

    utils.log(f"Found {len(nodes_df)} feature nodes", level=lg.INFO)

    # Create area nodes for spatial filtering of ways
    conn.execute(f"""
        CREATE TEMP TABLE feature_area_nodes AS
        SELECT id FROM osm_data
        WHERE kind = 'node'
        AND ST_Within(
            ST_Point(lon, lat),
            ST_GeomFromText('{polygon_wkt}')
        )
    """)

    # --- Way geometry construction using UNNEST + ST_MakeLine ---

    # Step 1: Find tagged ways with at least one node in spatial area
    conn.execute(f"""
        CREATE TEMP TABLE tagged_ways AS
        SELECT id, tags, refs
        FROM osm_data
        WHERE kind = 'way'
        AND ({tag_filter})
        AND refs IS NOT NULL AND array_length(refs) > 0
        AND EXISTS (
            SELECT 1 FROM UNNEST(refs) AS t(node_id)
            WHERE node_id IN (SELECT id FROM feature_area_nodes)
        )
    """)

    # Step 2: Unnest way refs with ordering
    conn.execute("""
        CREATE TEMP TABLE matching_ways_with_node_refs AS
        SELECT id, UNNEST(refs) AS ref, UNNEST(range(length(refs))) AS ref_idx
        FROM osm_data
        SEMI JOIN tagged_ways USING (id)
        WHERE kind = 'way'
    """)

    # Step 3: Build node point geometries for referenced nodes
    conn.execute("""
        CREATE TEMP TABLE required_nodes_with_geometries AS
        SELECT id, ST_Point(lon, lat) AS geometry
        FROM osm_data nodes
        SEMI JOIN matching_ways_with_node_refs ON nodes.id = matching_ways_with_node_refs.ref
        WHERE kind = 'node'
    """)

    # Step 4: Construct linestrings using ST_MakeLine
    conn.execute("""
        CREATE TEMP TABLE matching_ways_linestrings AS
        SELECT
            tagged_ways.id,
            tagged_ways.tags,
            tagged_ways.refs,
            ST_MakeLine(list(nodes.geometry ORDER BY ref_idx ASC)) AS linestring
        FROM tagged_ways
        JOIN matching_ways_with_node_refs
            ON tagged_ways.id = matching_ways_with_node_refs.id
        JOIN required_nodes_with_geometries nodes
            ON matching_ways_with_node_refs.ref = nodes.id
        GROUP BY tagged_ways.id, tagged_ways.tags, tagged_ways.refs
    """)

    # Step 5: Determine polygon vs linestring using full OSM wiki rules
    polygon_case = _build_polygon_case_sql()
    ways_df = conn.execute(f"""
        WITH way_polygon_feature AS (
            SELECT id,
                (ST_Equals(ST_StartPoint(linestring), ST_EndPoint(linestring))
                 AND tags IS NOT NULL
                 AND ({polygon_case})
                ) AS is_polygon
            FROM matching_ways_linestrings
        )
        SELECT
            mwl.id,
            mwl.tags,
            mwl.refs,
            ST_AsWKB(
                CASE WHEN wpf.is_polygon
                    THEN ST_MakePolygon(mwl.linestring)
                    ELSE mwl.linestring
                END
            ) AS geometry,
            wpf.is_polygon
        FROM matching_ways_linestrings mwl
        JOIN way_polygon_feature wpf ON mwl.id = wpf.id
    """).fetch_arrow_table()

    utils.log(f"Found {len(ways_df)} feature ways", level=lg.INFO)

    # --- Relation geometry construction in SQL ---

    # Step R1: Select matching relations (boundary or multipolygon type)
    conn.execute(f"""
        CREATE TEMP TABLE matching_relations AS
        SELECT id, tags
        FROM osm_data
        WHERE kind = 'relation'
        AND ({tag_filter})
        AND len(refs) > 0
        AND tags IS NOT NULL AND cardinality(tags) > 0
        AND list_contains(map_keys(tags), 'type')
        AND list_has_any(map_extract(tags, 'type'), ['boundary', 'multipolygon'])
    """)

    # Step R2: Unnest relation refs, keep only way refs
    conn.execute("""
        CREATE TEMP TABLE matching_relations_with_ways_refs AS
        WITH unnested AS (
            SELECT r.id,
                UNNEST(refs) AS ref,
                UNNEST(ref_types) AS ref_type,
                UNNEST(ref_roles) AS ref_role,
                UNNEST(range(length(refs))) AS ref_idx
            FROM osm_data r
            SEMI JOIN matching_relations USING (id)
            WHERE kind = 'relation'
        )
        SELECT id, ref, ref_role, ref_idx
        FROM unnested
        WHERE ref_type = 'way'
    """)

    # Step R3: Get linestrings for ways referenced by relations
    conn.execute("""
        CREATE TEMP TABLE required_ways_linestrings AS
        WITH ways_refs AS (
            SELECT id,
                UNNEST(refs) AS ref,
                UNNEST(range(length(refs))) AS ref_idx
            FROM osm_data ways
            SEMI JOIN matching_relations_with_ways_refs
                ON ways.id = matching_relations_with_ways_refs.ref
            WHERE kind = 'way'
        ),
        nodes AS (
            SELECT id, ST_Point(lon, lat) AS geometry
            FROM osm_data
            SEMI JOIN ways_refs ON osm_data.id = ways_refs.ref
            WHERE kind = 'node'
        )
        SELECT ways_refs.id,
            ST_MakeLine(list(nodes.geometry ORDER BY ref_idx ASC)) AS linestring
        FROM ways_refs
        JOIN nodes ON ways_refs.ref = nodes.id
        GROUP BY ways_refs.id
    """)

    # Step R4: Join way linestrings with relations, handle role assignment
    conn.execute("""
        CREATE TEMP TABLE matching_relations_with_ways_linestrings AS
        WITH joined AS (
            SELECT r.id,
                COALESCE(r.ref_role, 'outer') AS ref_role,
                r.ref,
                w.linestring::GEOMETRY AS geometry
            FROM matching_relations_with_ways_refs r
            JOIN required_ways_linestrings w ON w.id = r.ref
            ORDER BY r.id, r.ref_idx
        ),
        any_outer AS (
            SELECT id, bool_or(ref_role = 'outer') AS has_any_outer
            FROM joined
            GROUP BY id
        )
        SELECT j.* EXCLUDE (ref_role),
            CASE WHEN ao.has_any_outer THEN j.ref_role ELSE 'outer' END AS ref_role
        FROM joined j
        JOIN any_outer ao ON ao.id = j.id
    """)

    # Step R5: Merge linestrings, split into polygons
    conn.execute("""
        CREATE TEMP TABLE matching_relations_with_merged_polygons AS
        WITH merged AS (
            SELECT id, ref_role,
                UNNEST(
                    ST_Dump(ST_LineMerge(ST_Collect(list(geometry)))),
                    recursive := true
                )
            FROM matching_relations_with_ways_linestrings
            GROUP BY id, ref_role
        ),
        with_linestrings AS (
            SELECT id, ref_role, geom AS geometry,
                row_number() OVER (PARTITION BY id) AS geometry_id
            FROM merged
            WHERE ST_NPoints(geom) >= 4
        ),
        valid AS (
            SELECT id FROM (
                SELECT id,
                    bool_and(
                        ST_Equals(ST_StartPoint(geometry), ST_EndPoint(geometry))
                    ) AS is_valid
                FROM with_linestrings
                GROUP BY id
            ) WHERE is_valid
        )
        SELECT id, ref_role, ST_MakePolygon(geometry) AS geometry, geometry_id
        FROM with_linestrings
        SEMI JOIN valid USING (id)
    """)

    # Step R6: Subtract inner holes from outer polygons
    conn.execute("""
        CREATE TEMP TABLE outer_with_holes AS
        WITH outer_p AS (
            SELECT id, geometry_id, geometry
            FROM matching_relations_with_merged_polygons
            WHERE ref_role = 'outer'
        ),
        inner_p AS (
            SELECT id, geometry_id, geometry
            FROM matching_relations_with_merged_polygons
            WHERE ref_role = 'inner'
        )
        SELECT op.id, op.geometry_id,
            ST_Difference(
                any_value(op.geometry),
                ST_Union_Agg(ip.geometry)
            ) AS geometry
        FROM outer_p op
        JOIN inner_p ip
            ON op.id = ip.id AND ST_Within(ip.geometry, op.geometry)
        GROUP BY op.id, op.geometry_id
    """)

    conn.execute("""
        CREATE TEMP TABLE outer_without_holes AS
        WITH outer_p AS (
            SELECT id, geometry_id, geometry
            FROM matching_relations_with_merged_polygons
            WHERE ref_role = 'outer'
        )
        SELECT op.id, op.geometry_id, op.geometry
        FROM outer_p op
        ANTI JOIN outer_with_holes owh
            ON op.id = owh.id AND op.geometry_id = owh.geometry_id
    """)

    # Step R7: Union all outer polygons per relation and join with tags
    relations_df = conn.execute("""
        SELECT r.id, r.tags,
            ST_AsWKB(ST_Union_Agg(g.geometry)) AS geometry
        FROM (
            SELECT id, geometry FROM outer_with_holes
            UNION ALL
            SELECT id, geometry FROM outer_without_holes
        ) g
        JOIN matching_relations r ON r.id = g.id
        GROUP BY r.id, r.tags
    """).fetch_arrow_table()

    utils.log(f"Found {len(relations_df)} feature relations", level=lg.INFO)

    if len(nodes_df) == 0 and len(ways_df) == 0 and len(relations_df) == 0:
        msg = f"No feature data found in {pbf_path} for the specified area and tags"
        raise InsufficientResponseError(msg)

    return nodes_df, ways_df, relations_df