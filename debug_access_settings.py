#!/usr/bin/env python3
"""
Debug script to check the access settings.
"""

import ducknx as dx

def main():
    print("Debugging access settings")
    print("=" * 30)
    
    print(f"default_access setting: {repr(dx.settings.default_access)}")
    
    # Check what's actually in the setting
    print(f"Contains 'private': {'private' in dx.settings.default_access}")
    
    # Let's also check some basic highway counts without any access filter
    import duckdb
    from pathlib import Path
    
    pbf_file = Path("/Users/frankfortunat/repos/duck_osmnx/berlin-latest.osm.pbf")
    
    conn = duckdb.connect()
    try:
        conn.execute("INSTALL spatial")
        conn.execute("LOAD spatial")
        
        print(f"\n--- Basic highway counts (no access filter) ---")
        basic_query = f"""
        SELECT 
            tags['highway'] as highway_type,
            COUNT(*) as count
        FROM ST_ReadOSM('{pbf_file}')
        WHERE kind = 'way' 
        AND tags['highway'] IS NOT NULL
        AND tags['area'] != 'yes'
        GROUP BY tags['highway']
        ORDER BY count DESC
        LIMIT 15
        """
        
        result = conn.execute(basic_query).fetchall()
        for row in result:
            print(f"  {row[0]}: {row[1]}")
        
        print(f"\n--- Access tag distribution ---")
        access_query = f"""
        SELECT 
            tags['access'] as access_type,
            COUNT(*) as count
        FROM ST_ReadOSM('{pbf_file}')
        WHERE kind = 'way' 
        AND tags['highway'] IS NOT NULL
        GROUP BY tags['access']
        ORDER BY count DESC
        LIMIT 10
        """
        
        result = conn.execute(access_query).fetchall()
        for row in result:
            print(f"  {row[0] if row[0] else 'NULL'}: {row[1]}")
            
    except Exception as e:
        print(f"Error: {e}")
        print(f"Error type: {type(e).__name__}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()