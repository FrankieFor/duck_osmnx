"""Tools to work with local OSM PBF files using DuckDB."""

from __future__ import annotations

import logging as lg
from pathlib import Path

import duckdb
import pandas as pd
from shapely import MultiPolygon
from shapely import Polygon

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


def _polygon_to_wkt(polygon: Polygon | MultiPolygon) -> str:
    """
    Convert a Shapely polygon to WKT format for DuckDB queries.
    
    Parameters
    ----------
    polygon
        The polygon to convert.
        
    Returns
    -------
    wkt_string
        The polygon in Well-Known Text format.
    """
    return polygon.wkt


def _read_pbf_network_duckdb(
    polygon: Polygon | MultiPolygon,
    network_type: str,
    custom_filter: str | list[str] | None,
    pbf_path: str | Path,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Retrieve networked ways and nodes from a local PBF file using DuckDB SQL.

    This optimized code path performs filtering and joins in DuckDB and returns
    pandas DataFrames directly, avoiding intermediate JSON dict conversion.

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
        DataFrames of nodes and ways.

    Raises
    ------
    InsufficientResponseError
        If no data was retrieved from the PBF file.
    """
    pbf_path = Path(pbf_path)
    if not pbf_path.exists():
        msg = f"PBF file not found: {pbf_path}"
        raise FileNotFoundError(msg)

    polygon_wkt = _polygon_to_wkt(polygon)
    network_filter = _get_network_filter_sql(network_type)

    if custom_filter:
        if isinstance(custom_filter, list):
            custom_filter = " AND ".join(custom_filter)
        network_filter += f" AND ({custom_filter})"

    conn = duckdb.connect()
    try:
        conn.execute("INSTALL spatial")
        conn.execute("LOAD spatial")

        # Load PBF into temp table once
        conn.execute(f"CREATE TEMP TABLE osm_data AS SELECT * FROM ST_ReadOSM('{pbf_path}')")

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
            f"tags['{tag}'] AS \"{tag}\"" for tag in settings.useful_tags_way
        )
        ways_df = conn.execute(f"""
            SELECT
                id AS osmid,
                refs,
                {way_tag_cols}
            FROM filtered_ways
        """).fetchdf()

        if ways_df.empty:
            msg = f"No ways found matching network filter in {pbf_path}"
            raise InsufficientResponseError(msg)

        utils.log(f"Found {len(ways_df)} ways matching network filter", level=lg.INFO)

        # Get ALL nodes referenced by filtered ways (not just in area)
        node_tag_cols = ", ".join(
            f"n.tags['{tag}'] AS \"{tag}\"" for tag in settings.useful_tags_node
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
        """).fetchdf()

        if nodes_df.empty:
            msg = "No nodes found for the filtered ways"
            raise InsufficientResponseError(msg)

        utils.log(f"Found {len(nodes_df)} nodes for filtered ways", level=lg.INFO)

    finally:
        conn.close()

    return nodes_df, ways_df


def _read_pbf_features_duckdb(
    polygon: Polygon | MultiPolygon,
    tags: dict[str, bool | str | list[str]],
    pbf_path: str | Path,
) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Retrieve OSM features from a local PBF file using DuckDB SQL.

    This optimized code path performs filtering and geometry construction in
    DuckDB and returns pandas DataFrames directly.

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
        DataFrames of nodes, ways, and relations.

    Raises
    ------
    InsufficientResponseError
        If no data was retrieved from the PBF file.
    """
    pbf_path = Path(pbf_path)
    if not pbf_path.exists():
        msg = f"PBF file not found: {pbf_path}"
        raise FileNotFoundError(msg)

    polygon_wkt = _polygon_to_wkt(polygon)

    # Build tag filter conditions
    tag_conditions = []
    for key, value in tags.items():
        if isinstance(value, bool):
            if value:
                tag_conditions.append(f"tags['{key}'] IS NOT NULL")
            else:
                tag_conditions.append(f"tags['{key}'] IS NULL")
        elif isinstance(value, str):
            tag_conditions.append(f"tags['{key}'] = '{value}'")
        elif isinstance(value, list):
            value_list = "', '".join(value)
            tag_conditions.append(f"tags['{key}'] IN ('{value_list}')")

    tag_filter = " OR ".join(tag_conditions) if tag_conditions else "1=1"

    conn = duckdb.connect()
    try:
        conn.execute("INSTALL spatial")
        conn.execute("LOAD spatial")

        # Load PBF into temp table once
        conn.execute(f"CREATE TEMP TABLE osm_data AS SELECT * FROM ST_ReadOSM('{pbf_path}')")

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
        """).fetchdf()

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

        # Step 5: Determine polygon vs linestring and output WKB
        ways_df = conn.execute("""
            WITH way_polygon_feature AS (
                SELECT id,
                    (ST_Equals(ST_StartPoint(linestring), ST_EndPoint(linestring))
                     AND tags IS NOT NULL
                     AND NOT (
                         list_contains(map_keys(tags), 'area')
                         AND list_extract(map_extract(tags, 'area'), 1) = 'no'
                     )
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
        """).fetchdf()

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
        """).fetchdf()

        utils.log(f"Found {len(relations_df)} feature relations", level=lg.INFO)

        if nodes_df.empty and ways_df.empty and relations_df.empty:
            msg = f"No feature data found in {pbf_path} for the specified area and tags"
            raise InsufficientResponseError(msg)

    finally:
        conn.close()

    return nodes_df, ways_df, relations_df