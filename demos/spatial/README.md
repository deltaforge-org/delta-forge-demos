# Geospatial Demos

SQL demos for spatial analytics using H3 hexagonal indexing and GIS operations. All queries run inside the DeltaForge GUI with built-in assertions.

## What's Covered

- **H3 Indexing** -- Lat/lng to cell, resolution levels, k-ring neighbors, hierarchical aggregation
- **Point-in-Polygon** -- WKT geometry, containment queries, boundary detection
- **Fleet Tracking** -- GPS trajectory analysis, speed/heading calculations, stop detection
- **Delivery Optimization** -- Route planning, coverage zones, service area analysis
- **Emergency Response** -- Station coverage, response time estimation, demand mapping
- **Maritime Shipping** -- Vessel tracking, port proximity, route analytics

## Running a Demo

1. Open the DeltaForge GUI
2. Select a demo from this category
3. Run **setup.sql** to create tables and load seed data
4. Step through **queries.sql** -- assertions verify each result
5. Run **cleanup.sql** to tear down
