#!/usr/bin/env python3
"""
Debug script to check the schema of ST_ReadOSM output.
"""

import duckdb
from pathlib import Path

def main():
    print("Debugging ST_ReadOSM schema")
    print("=" * 40)
    
    pbf_file = Path("/Users/frankfortunat/repos/duck_osmnx/berlin-latest.osm.pbf")
    print(f"PBF file: {pbf_file}")
    
    conn = duckdb.connect()
    try:
        # Install and load spatial extension
        print("Installing spatial extension...")
        conn.execute("INSTALL spatial")
        conn.execute("LOAD spatial")
        
        # Check schema by describing the ST_ReadOSM output
        print("\nChecking ST_ReadOSM schema...")
        result = conn.execute(f"DESCRIBE SELECT * FROM ST_ReadOSM('{pbf_file}') LIMIT 1").fetchall()
        
        print("Columns available:")
        for row in result:
            print(f"  - {row[0]}: {row[1]}")
        
        # Get a few sample rows to understand the data structure
        print(f"\nSample data (first 5 rows):")
        sample = conn.execute(f"SELECT * FROM ST_ReadOSM('{pbf_file}') LIMIT 5").fetchall()
        
        for i, row in enumerate(sample):
            print(f"\nRow {i+1}:")
            for j, col in enumerate(result):
                print(f"  {col[0]}: {row[j]}")
            if i >= 2:  # Only show first 3 rows to avoid too much output
                break
                
    except Exception as e:
        print(f"Error: {e}")
        print(f"Error type: {type(e).__name__}")
    finally:
        conn.close()

if __name__ == "__main__":
    main()