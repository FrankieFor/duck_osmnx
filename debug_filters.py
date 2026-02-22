#!/usr/bin/env python3
"""
Debug script to check the network filters.
"""

import duckdb
from pathlib import Path
from ducknx._pbf_reader import _get_network_filter_sql

def main():
    print("Debugging network filters")
    print("=" * 40)
    
    pbf_file = Path("/Users/frankfortunat/repos/duck_osmnx/berlin-latest.osm.pbf")
    
    network_types = ["drive", "walk", "bike"]
    
    conn = duckdb.connect()
    try:
        conn.execute("INSTALL spatial")
        conn.execute("LOAD spatial")
        
        for network_type in network_types:
            print(f"\n--- {network_type.upper()} Network Filter ---")
            
            # Generate the filter
            network_filter = _get_network_filter_sql(network_type)
            print(f"Filter: {network_filter}")
            
            # Test the filter
            test_query = f"""
            SELECT COUNT(*) as matching_ways
            FROM ST_ReadOSM('{pbf_file}')
            WHERE kind = 'way' 
            AND {network_filter}
            LIMIT 10
            """
            
            try:
                result = conn.execute(test_query).fetchone()
                print(f"Matching ways: {result[0]}")
                
                if result[0] > 0:
                    # Get some examples
                    example_query = f"""
                    SELECT tags['highway'], tags['access'], tags['motor_vehicle'], tags['motorcar'], COUNT(*)
                    FROM ST_ReadOSM('{pbf_file}')
                    WHERE kind = 'way' 
                    AND {network_filter}
                    GROUP BY tags['highway'], tags['access'], tags['motor_vehicle'], tags['motorcar']
                    ORDER BY COUNT(*) DESC
                    LIMIT 5
                    """
                    
                    examples = conn.execute(example_query).fetchall()
                    print("Example matches:")
                    for row in examples:
                        print(f"  highway={row[0]}, access={row[1]}, motor_vehicle={row[2]}, motorcar={row[3]}, count={row[4]}")
                
            except Exception as e:
                print(f"Filter test failed: {e}")
    
    except Exception as e:
        print(f"Error: {e}")
        print(f"Error type: {type(e).__name__}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()