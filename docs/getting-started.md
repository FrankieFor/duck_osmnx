# Getting Started

## Get Started in 4 Steps

1. Install ducknx by following the [Installation](installation.md) guide.

2. Read the [Introducing ducknx](#introducing-ducknx) section on this page.

3. Work through the ducknx [Examples Gallery](https://github.com/gboeing/osmnx-examples) for step-by-step tutorials and sample code.

4. Consult the [User Reference](user-reference.md) for complete details on using the package.

Finally, if you're not already familiar with [NetworkX](https://networkx.org) and [GeoPandas](https://geopandas.org), make sure you read their user guides as ducknx uses their data structures.

## Introducing ducknx

This quick introduction explains key concepts and the basic functionality of ducknx.

### Overview

ducknx is built on top of NetworkX and GeoPandas, and reads [OpenStreetMap](https://www.openstreetmap.org) data from local PBF files to:

* Extract and model street networks or other infrastructure with a single line of code
* Extract geospatial features (e.g., political boundaries, building footprints, grocery stores, transit stops) as a GeoDataFrame
* Query by city name, polygon, bounding box, or point/address + distance
* Model driving, walking, biking, and other travel modes
* Attach node elevations from a local raster file or web service and calculate edge grades
* Impute missing speeds and calculate graph edge travel times
* Simplify and correct the network's topology to clean-up nodes and consolidate complex intersections
* Fast map-matching of points, routes, or trajectories to nearest graph edges or nodes
* Save/load network to/from disk as GraphML, GeoPackage, or OSM XML file
* Conduct topological and spatial analyses to automatically calculate dozens of indicators
* Calculate and visualize street bearings and orientations
* Calculate and visualize shortest-path routes that minimize distance, travel time, elevation, etc
* Explore street networks and geospatial features as a static map or interactive web map
* Visualize travel distance and travel time with isoline and isochrone maps
* Plot figure-ground diagrams of street networks and building footprints

The ducknx [Examples Gallery](https://github.com/gboeing/osmnx-examples) contains tutorials and demonstrations of all these features, and package usage is detailed in the [User Reference](user-reference.md).

### Configuration

You can configure ducknx using the `settings` module. Here you can adjust logging behavior, caching, server endpoints, and more. You can also configure ducknx to retrieve historical snapshots of OpenStreetMap data as of a certain date.

Read more about the [settings](user-reference.md#ducknx.settings) module in the User Reference.

### Geocoding and Querying

ducknx geocodes place names and addresses with the OpenStreetMap [Nominatim](https://nominatim.org) API. You can use the `geocoder` module to geocode place names or addresses to lat-lon coordinates. Or, you can retrieve place boundaries or any other OpenStreetMap elements by name or ID. Read more about the [geocoder](user-reference.md#ducknx.geocoder) module in the User Reference.

Using the `features` and `graph` modules, as described below, you can query data by lat-lon point, address, bounding box, bounding polygon, or place name (e.g., neighborhood, city, county, etc).

### Urban Amenities

Using ducknx's `features` module, you can extract any geospatial [features](https://wiki.openstreetmap.org/wiki/Map_features) (such as building footprints, grocery stores, schools, public parks, transit stops, etc) from a local OSM PBF file as a GeoPandas GeoDataFrame. This uses OpenStreetMap [tags](https://wiki.openstreetmap.org/wiki/Tags) to search for matching [elements](https://wiki.openstreetmap.org/wiki/Elements).

Read more about the [features](user-reference.md#ducknx.features) module in the User Reference.

### Modeling a Network

Using ducknx's `graph` module, you can extract any spatial network data (such as streets, paths, rail, canals, etc) from a local OSM PBF file and model them as NetworkX [MultiDiGraphs](https://networkx.org/documentation/stable/reference/classes/multidigraph.html).

In short, MultiDiGraphs are nonplanar directed graphs with possible self-loops and parallel edges. Thus, a one-way street will be represented with a single directed edge from node *u* to node *v*, but a bidirectional street will be represented with two reciprocal directed edges (with identical geometries): one from node *u* to node *v* and another from *v* to *u*, to represent both possible directions of flow. Because these graphs are nonplanar, they correctly model the topology of interchanges, bridges, and tunnels. That is, edge crossings in a two-dimensional plane are not intersections in a ducknx model unless they represent true junctions in the three-dimensional real world.

The `graph` module uses filters to query the local PBF data: you can either specify a built-in network type or provide your own custom filter. Under the hood, ducknx does several things to generate the best possible model. It initially creates a 500m-buffered graph before truncating it to your desired query area, to ensure accurate streets-per-node stats and to attenuate graph perimeter effects. By default, it returns the largest weakly connected component. It also simplifies the graph topology as discussed below.

Read more about the [graph](user-reference.md#ducknx.graph) module in the User Reference and refer to the official reference paper at the [Further Reading](further-reading.md) page for complete modeling details.

### Topology Clean-Up

The `simplification` module automatically processes the network's topology from the original raw OpenStreetMap data, such that nodes represent intersections/dead-ends and edges represent the street segments that link them. This takes two primary forms: graph simplification and intersection consolidation.

**Graph simplification** cleans up the graph's topology so that nodes represent intersections or dead-ends and edges represent street segments. This is important because in OpenStreetMap raw data, ways comprise sets of straight-line segments between nodes: that is, nodes are vertices for streets' curving line geometries, not just intersections and dead-ends. By default, ducknx simplifies this topology by discarding non-intersection/dead-end nodes while retaining the complete true edge geometry as an edge attribute. When multiple OpenStreetMap ways are merged into a single graph edge, the ways' attribute values can be aggregated into a single value.

**Intersection consolidation** is important because many real-world street networks feature complex intersections and traffic circles, resulting in a cluster of graph nodes where there is really just one true intersection as we would think of it in transportation or urban design. Similarly, divided roads are often represented by separate centerline edges: the intersection of two divided roads thus creates 4 nodes, representing where each edge intersects a perpendicular edge, but these 4 nodes represent a single intersection in the real world. ducknx can consolidate such complex intersections into a single node and optionally rebuild the graph's edge topology accordingly. When multiple OpenStreetMap nodes are merged into a single graph node, the nodes' attribute values can be aggregated into a single value.

Read more about the [simplification](user-reference.md#ducknx.simplification) module in the User Reference.

### Model Attributes

A ducknx model has some standard required attributes, plus some optional attributes. The latter are sometimes present based on the source OSM data's tagging, the `settings` module configuration, and any processing you may have done to add additional attributes (as noted in various functions' documentation).

As a NetworkX [MultiDiGraph](https://networkx.org/documentation/stable/reference/classes/multidigraph.html) object, it has top-level `graph`, `nodes`, and `edges` attributes. The `graph` attribute dictionary must contain a "crs" key defining its coordinate reference system. The `nodes` are identified by OSM ID and each must contain a `data` attribute dictionary that must have "x" and "y" keys defining its coordinates and a "street_count" key defining how many physical streets are incident to it. The `edges` are identified by a 3-tuple of "u" (source node ID), "v" (target node ID), and "key" (to differentiate parallel edges), and each must contain a `data` attribute dictionary that must have an "osmid" key defining its OSM ID and a "length" key defining its length in meters.

The ducknx `graph` module automatically creates MultiDiGraphs with these required attributes, plus additional optional attributes based on the `settings` module configuration. If you instead manually create your own graph model, make sure it has these required attributes at a minimum.

### Convert, Project, Save

ducknx's `convert` module can convert a MultiDiGraph to a [DiGraph](https://networkx.org/documentation/stable/reference/classes/digraph.html) if you prefer a directed representation of the network without any parallel edges, or to a [MultiGraph](https://networkx.org/documentation/stable/reference/classes/multigraph.html) if you need an undirected representation for use with functions or algorithms that only accept a MultiGraph object. If you just want a fully bidirectional graph (such as for a walking network), just configure the `settings` module's `bidirectional_network_types` before creating your graph.

The `convert` module can also convert a MultiDiGraph to/from GeoPandas node and edge [GeoDataFrames](https://geopandas.org/en/stable/docs/reference/geodataframe.html). The nodes GeoDataFrame is indexed by OSM ID and the edges GeoDataFrame is multi-indexed by `u, v, key` just like a NetworkX edge. This allows you to load arbitrary node/edge ShapeFiles or GeoPackage layers as GeoDataFrames then model them as a MultiDiGraph for graph analysis. Read more about the [convert](user-reference.md#ducknx.convert) module in the User Reference.

You can easily project your graph to different coordinate reference systems using the `projection` module. If you're unsure which [CRS](https://en.wikipedia.org/wiki/Coordinate_reference_system) you want to project to, ducknx can automatically determine an appropriate UTM CRS for you. Read more about the [projection](user-reference.md#ducknx.projection) module in the User Reference.

Using the `io` module, you can save your graph to disk as a GraphML file (to load into other network analysis software), a GeoPackage (to load into other GIS software), or an OSM XML file. Use the GraphML format whenever saving a graph for later work with ducknx. Read more about the [io](user-reference.md#ducknx.io) module in the User Reference.

### Network Measures

You can use the `stats` module to calculate a variety of geometric and topological measures as well as street network bearing and orientation statistics. These measures define streets as the edges in an undirected representation of the graph to prevent double-counting bidirectional edges of a two-way street. You can easily generate common stats in transportation studies, urban design, and network science, including intersection density, circuity, average node degree (connectedness), betweenness centrality, and much more. Read more about the [stats](user-reference.md#ducknx.stats) module in the User Reference.

You can also use NetworkX directly to calculate additional topological network measures.

### Working with Elevation

The `elevation` module lets you automatically attach elevations to the graph's nodes from a local raster file or the Google Maps [Elevation API](https://developers.google.com/maps/documentation/elevation) (or equivalent web API with a compatible interface). You can also calculate edge grades (i.e., rise-over-run) and analyze the steepness of certain streets or routes.

Read more about the [elevation](user-reference.md#ducknx.elevation) module in the User Reference.

### Routing

The `distance` module can find the nearest node(s) or edge(s) to coordinates using a fast spatial index. The `routing` module can solve shortest paths for network routing, parallelized with multiprocessing, using different weights (e.g., distance, travel time, elevation change, etc). It can also impute missing speeds to the graph edges. This imputation can obviously be imprecise, so the user can override it by passing in arguments that define local speed limits. It can also calculate free-flow travel times for each edge.

Read more about the [distance](user-reference.md#ducknx.distance) and [routing](user-reference.md#ducknx.routing) modules in the User Reference.

### Visualization

You can plot graphs, routes, network figure-ground diagrams, building footprints, and street network orientation rose diagrams (aka, polar histograms) with the `plot` module. You can also explore street networks, routes, or geospatial features as interactive [Folium](https://python-visualization.github.io/folium/) web maps.

Read more about the [plot](user-reference.md#ducknx.plot) module in the User Reference.

### Usage Limits

ducknx reads data from local PBF files and does not query the Overpass API. However, the `_from_address` and `_from_place` query methods use the Nominatim geocoding API. Refer to the [Nominatim Usage Policy](https://operations.osmfoundation.org/policies/nominatim/) for API usage limits and restrictions to which you must adhere.

## More Info

All of this functionality is demonstrated step-by-step in the ducknx [Examples Gallery](https://github.com/gboeing/osmnx-examples), and usage is detailed in the [User Reference](user-reference.md). Feature development details are in the [Changelog](https://github.com/gboeing/osmnx/blob/main/CHANGELOG.md). Consult the [Further Reading](further-reading.md) resources for additional technical details and research.

## Frequently Asked Questions

*How do I install ducknx?* Follow the [Installation](installation.md) guide.

*How do I use ducknx?* Check out the step-by-step tutorials in the ducknx [Examples Gallery](https://github.com/gboeing/osmnx-examples).

*How does this or that function work?* Consult the [User Reference](user-reference.md).

*What can I do with ducknx?* Check out recent [projects](https://geoffboeing.com/2018/03/osmnx-features-roundup) that use ducknx.

*I have a usage question.* Please ask it on [StackOverflow](https://stackoverflow.com/search?q=osmnx).
