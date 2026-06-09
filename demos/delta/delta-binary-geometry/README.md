# Delta Binary & Spatial Data Types

Demonstrates how Delta tables handle binary-like data (SHA-256 content hashes)
and spatial geometry patterns (WKT strings with coordinates).

## Data Story

A document management system stores file metadata with SHA-256 content hashes
that serve as binary fingerprints for each file. Meanwhile, a location service
stores spatial data as WKT (Well-Known Text) geometry strings, covering
landmarks, parks, and routes worldwide. This demonstrates how Delta tables
handle hash-like strings, coordinate data, and spatial geometry patterns.

## Tables

| Object | Type | Rows | Purpose |
|--------|------|------|---------|
| `document_store` | Delta Table | 23 (final) | File metadata with SHA-256 content hashes |
| `geo_locations` | Delta Table | 30 (final) | Spatial data with WKT geometry strings |

## Schemas

**document_store:** `id INT, name VARCHAR, mime_type VARCHAR, content_hash VARCHAR, size_bytes INT, created_at VARCHAR`

**geo_locations:** `id INT, name VARCHAR, loc_type VARCHAR, wkt VARCHAR, latitude DOUBLE, longitude DOUBLE, region VARCHAR`

## Operations

1. INSERT 25 documents with SHA-256 hashes and file metadata
2. INSERT 20 POINT locations (cities, landmarks) with lat/lon coordinates
3. INSERT 10 POLYGON and LINESTRING locations (parks, routes, boundaries)
4. UPDATE 3 location regions (reclassification)
5. DELETE 2 deprecated document entries

## Geometry Types

- **POINT:** Single coordinate locations (e.g., `POINT(-73.9857 40.7484)`)
- **POLYGON:** Area boundaries (e.g., Central Park, Hyde Park, Serengeti)
- **LINESTRING:** Route paths (e.g., Route 66, Rhine River, Silk Road)

## Verification

8 automated PASS/FAIL checks verify document counts, geometry type
distribution, SHA-256 hash integrity (64-char length), WKT format
validation, and region diversity.
