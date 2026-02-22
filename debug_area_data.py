#!/usr/bin/env python3
"""
Debug script to check what OSM data exists in the test area.
"""

import duckdb
from pathlib import Path

def main():
    print("Debugging OSM data in test area")
    print("=" * 40)
    
    pbf_file = Path("/Users/frankfortunat/repos/duck_osmnx/berlin-latest.osm.pbf")
    
    # Test area coordinates
    north, south, east, west = 52.525, 52.520, 13.415, 13.405
    polygon_wkt = f"POLYGON(({west} {south}, {east} {south}, {east} {north}, {west} {north}, {west} {south}))"
    
    print(f"Test area: North={north}, South={south}, East={east}, West={west}")
    print(f"Polygon WKT: {polygon_wkt}")
    
    conn = duckdb.connect()
    try:
        conn.execute("INSTALL spatial")
        conn.execute("LOAD spatial")
        
        # Check total counts in area
        print(f"\n--- Total elements in area ---")
        
        counts_query = f"""
        SELECT 
            kind,
            COUNT(*) as count
        FROM ST_ReadOSM('{pbf_file}')
        WHERE ST_Within(
            ST_Point(lon, lat),
            ST_GeomFromText('{polygon_wkt}')
        )
        GROUP BY kind
        ORDER BY count DESC
        """
        
        result = conn.execute(counts_query).fetchall()
        for row in result:
            print(f"  {row[0]}: {row[1]}")
        
        # Check ways with highway tags in area
        print(f"\n--- Ways with highway tags in area ---")
        
        highway_query = f"""
        SELECT 
            COUNT(*) as highway_ways,
            COUNT(DISTINCT tags['highway']) as distinct_highway_types
        FROM ST_ReadOSM('{pbf_file}')
        WHERE kind = 'way' 
        AND tags['highway'] IS NOT NULL
        AND EXISTS (
            SELECT 1 FROM UNNEST(refs) AS node_ref
            WHERE EXISTS (
                SELECT 1 FROM ST_ReadOSM('{pbf_file}') nodes
                WHERE nodes.kind = 'node' 
                AND nodes.id = node_ref
                AND ST_Within(ST_Point(nodes.lon, nodes.lat), ST_GeomFromText('{polygon_wkt}'))
            )
        )
        """
        
        result = conn.execute(highway_query).fetchall()
        for row in result:
            print(f"  Highway ways: {row[0]}")
            print(f"  Distinct highway types: {row[1]}")
        
        # Show some example highway types
        print(f"\n--- Example highway types in area ---")
        
        highway_types_query = f"""
        SELECT 
            tags['highway'] as highway_type,
            COUNT(*) as count
        FROM ST_ReadOSM('{pbf_file}')
        WHERE kind = 'way' 
        AND tags['highway'] IS NOT NULL
        AND EXISTS (
            SELECT 1 FROM UNNEST(refs) AS node_ref
            WHERE EXISTS (
                SELECT 1 FROM ST_ReadOSM('{pbf_file}') nodes
                WHERE nodes.kind = 'node' 
                AND nodes.id = node_ref
                AND ST_Within(ST_Point(nodes.lon, nodes.lat), ST_GeomFromText('{polygon_wkt}'))
            )
        )
        GROUP BY tags['highway']
        ORDER BY count DESC
        LIMIT 10
        """
        
        result = conn.execute(highway_types_query).fetchall()
        for row in result:
            print(f"  {row[0]}: {row[1]}")
            
    except Exception as e:
        print(f"Error: {e}")
        print(f"Error type: {type(e).__name__}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()