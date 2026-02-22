#!/usr/bin/env python3
"""
Simplified debug script to check OSM data in the test area.
"""

import duckdb
from pathlib import Path

def main():
    print("Debugging OSM data in test area (simplified)")
    print("=" * 50)
    
    pbf_file = Path("/Users/frankfortunat/repos/duck_osmnx/berlin-latest.osm.pbf")
    
    # Test area coordinates  
    north, south, east, west = 52.525, 52.520, 13.415, 13.405
    polygon_wkt = f"POLYGON(({west} {south}, {east} {south}, {east} {north}, {west} {north}, {west} {south}))"
    
    print(f"Test area: North={north}, South={south}, East={east}, West={west}")
    
    conn = duckdb.connect()
    try:
        conn.execute("INSTALL spatial")
        conn.execute("LOAD spatial")
        
        # Check nodes in area
        print(f"\n--- Nodes in area ---")
        nodes_query = f"""
        SELECT COUNT(*) as node_count
        FROM ST_ReadOSM('{pbf_file}')
        WHERE kind = 'node'
        AND ST_Within(ST_Point(lon, lat), ST_GeomFromText('{polygon_wkt}'))
        """
        
        result = conn.execute(nodes_query).fetchone()
        print(f"  Nodes in area: {result[0]}")
        
        # Check ways (all ways, not filtered by area yet)
        print(f"\n--- Ways with highway tags (anywhere in file) ---")
        ways_query = f"""
        SELECT 
            tags['highway'] as highway_type,
            COUNT(*) as count
        FROM ST_ReadOSM('{pbf_file}')
        WHERE kind = 'way' 
        AND tags['highway'] IS NOT NULL
        GROUP BY tags['highway']
        ORDER BY count DESC
        LIMIT 10
        """
        
        result = conn.execute(ways_query).fetchall()
        print("  Top highway types in Berlin PBF:")
        for row in result:
            print(f"    {row[0]}: {row[1]}")
        
        # Let's try a broader area - maybe our area is too small
        print(f"\n--- Trying broader area (0.02 degree buffer) ---")
        
        buffer = 0.02
        broad_north = north + buffer
        broad_south = south - buffer  
        broad_east = east + buffer
        broad_west = west - buffer
        
        broad_polygon_wkt = f"POLYGON(({broad_west} {broad_south}, {broad_east} {broad_south}, {broad_east} {broad_north}, {broad_west} {broad_north}, {broad_west} {broad_south}))"
        
        broad_nodes_query = f"""
        SELECT COUNT(*) as node_count
        FROM ST_ReadOSM('{pbf_file}')
        WHERE kind = 'node'
        AND ST_Within(ST_Point(lon, lat), ST_GeomFromText('{broad_polygon_wkt}'))
        """
        
        result = conn.execute(broad_nodes_query).fetchone()
        print(f"  Nodes in broader area: {result[0]}")
        
        print(f"  Broader area: N={broad_north:.3f}, S={broad_south:.3f}, E={broad_east:.3f}, W={broad_west:.3f}")
        
    except Exception as e:
        print(f"Error: {e}")
        print(f"Error type: {type(e).__name__}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()