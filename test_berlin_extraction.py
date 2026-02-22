#!/usr/bin/env python3
"""
Test script to extract road network from berlin-latest.osm.pbf using the new DuckDB-based ducknx.
"""

import time
from pathlib import Path

# Import the modified ducknx
import ducknx as dx
from shapely.geometry import box

def main():
    print("Testing Berlin road network extraction with DuckDB-based ducknx")
    print("=" * 60)
    
    # Set up the PBF file path
    pbf_file = Path("/Users/frankfortunat/repos/duck_osmnx/berlin-latest.osm.pbf")
    print(f"PBF file: {pbf_file}")
    print(f"PBF file exists: {pbf_file.exists()}")
    print(f"PBF file size: {pbf_file.stat().st_size / 1024 / 1024:.1f} MB")
    
    # Configure ducknx to use the PBF file and enable logging
    dx.settings.pbf_file_path = str(pbf_file)
    dx.settings.log_console = True
    dx.settings.log_level = 20  # INFO level
    
    print(f"\nConfigured ducknx PBF path: {dx.settings.pbf_file_path}")
    
    # Define a larger bounding box in Berlin for testing
    # Using coordinates around Alexanderplatz area (expanded)
    north, south, east, west = 52.530, 52.515, 13.420, 13.400
    print(f"\nTest area: North={north}, South={south}, East={east}, West={west}")
    
    # Create a polygon for the bounding box
    bbox_poly = box(west, south, east, north)
    print(f"Bounding box polygon: {bbox_poly}")
    
    # Test different network types
    network_types = ["drive", "walk", "bike"]
    
    for network_type in network_types:
        print(f"\n--- Testing {network_type} network ---")
        start_time = time.time()
        
        try:
            # Extract network using graph_from_bbox (disable simplify to preserve small networks)
            print(f"Extracting {network_type} network...")
            bbox = (north, south, east, west)
            G = dx.graph_from_bbox(bbox, network_type=network_type, simplify=False)
            
            end_time = time.time()
            extraction_time = end_time - start_time
            
            print(f"✅ Success! Extracted {network_type} network in {extraction_time:.2f} seconds")
            print(f"   Nodes: {G.number_of_nodes()}")
            print(f"   Edges: {G.number_of_edges()}")
            
            # Get some basic stats
            if G.number_of_nodes() > 0:
                print(f"   First node: {list(G.nodes())[0]}")
                print(f"   Node attributes: {list(G.nodes(data=True))[0][1].keys()}")
            
            if G.number_of_edges() > 0:
                edge = list(G.edges(data=True))[0]
                print(f"   First edge: {edge[0]} -> {edge[1]}")
                print(f"   Edge attributes: {list(edge[2].keys())}")
        
        except Exception as e:
            end_time = time.time()
            extraction_time = end_time - start_time
            print(f"❌ Failed to extract {network_type} network after {extraction_time:.2f} seconds")
            print(f"   Error: {str(e)}")
            print(f"   Error type: {type(e).__name__}")
    
    print(f"\n{'=' * 60}")
    print("Test completed!")

if __name__ == "__main__":
    main()