-- ============================================================================
-- Induced Seismicity Monitoring - Incremental Load and Verification
-- ============================================================================
-- Three monthly pulls of the USGS earthquake catalogue over the Oklahoma
-- seismic zone and the Permian Basin, magnitude 2.5 and above:
--
--   2026-03-11  2026-01   34 events
--   2026-03-11  2026-02   47 events
--   2026-03-12  2026-03   64 events
--
-- The data is REAL and is in the public domain, being a work of the United
-- States government. Every value asserted below was computed from the files by
-- reading them as plain JSON before the engine saw them.
--
-- What the numbers say, and the reason a register like this exists: 117 of the
-- 145 events are on the Texas network and 20 on the Oklahoma network, which is
-- where produced water disposal is concentrated, and the count rises month on
-- month. One event reaches magnitude 4.3, the level at which a regulator
-- starts asking about injection rates.
-- ============================================================================


-- ============================================================================
-- 1. WHAT DISCOVER DECIDED
-- ============================================================================

DISCOVER {{zone_name}}.seismicity.seismic_feed
    PATH '{{data_subdir}}/landing'
    WITH (FILE_METADATA = true)
    PRINT;


-- ============================================================================
-- 2. ONE FEATURE, ONE ROW
-- ============================================================================
-- 145 events across three files. If the FeatureCollection were being read as
-- a single JSON object this would be 3, one row per file, and every event
-- would be buried in an array.

ASSERT ROW_COUNT = 145
SELECT *
FROM {{zone_name}}.seismicity.seismic_feed;


-- ============================================================================
-- 3. EACH PULL LANDED WHOLE
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE events = 34 WHERE df_file_name = '2026-03-11_seismicity_2026-01.geojson'
ASSERT VALUE events = 47 WHERE df_file_name = '2026-03-11_seismicity_2026-02.geojson'
ASSERT VALUE events = 64 WHERE df_file_name = '2026-03-12_seismicity_2026-03.geojson'
SELECT df_file_name, COUNT(*) AS events
FROM {{zone_name}}.seismicity.seismic_feed
GROUP BY df_file_name
ORDER BY df_file_name;


-- ============================================================================
-- 4. THE GEOMETRY IS KEPT WHOLE
-- ============================================================================
-- `$.geometry` is declared opaque in the profile, so a point's coordinate
-- array is one value. Every event has one, and no column was created per
-- ordinate. For a polygon layer the alternative would be a column per vertex,
-- which is how a geospatial table turns into ten thousand columns.

ASSERT ROW_COUNT = 1
ASSERT VALUE events = 145
ASSERT VALUE with_geometry = 145
ASSERT VALUE points = 145
SELECT COUNT(*)                                              AS events,
       COUNT(geometry)                                       AS with_geometry,
       COUNT(*) FILTER (WHERE geometry LIKE '%Point%')       AS points
FROM {{zone_name}}.seismicity.seismic_feed;


-- ============================================================================
-- 5. LOAD THE FIRST TWO PULLS
-- ============================================================================
-- Both January and February arrived on 11 March, so both load together.
--
-- The key is the event id, which RFC 7946 provides for exactly this purpose
-- and this feed sets. The file is not part of it: a catalogue is routinely
-- re-issued with a revised magnitude once an event has been reviewed, and
-- keying on the event means the revision lands on the event rather than beside
-- it.

MERGE INTO {{zone_name}}.seismicity.seismic_register AS t
USING (
    SELECT SUBSTRING(f.df_file_name, 23, 7)   AS catalogue_month,
           SUBSTRING(f.df_file_name, 1, 10)   AS delivered_on,
           f.df_file_name                     AS source_file,
           f.id                               AS event_id,
           f.properties_mag                   AS magnitude,
           f.properties_mag_type              AS magnitude_type,
           f.properties_place                 AS place,
           f.properties_time                  AS event_time,
           f.properties_sig                   AS significance,
           f.properties_net                   AS network,
           f.properties_type                  AS event_type,
           f.geometry
    FROM {{zone_name}}.seismicity.seismic_feed f
    WHERE f.df_file_name LIKE '2026-03-11%'
) AS s
ON t.event_id = s.event_id
WHEN MATCHED THEN
    UPDATE SET catalogue_month = s.catalogue_month,
               delivered_on    = s.delivered_on,
               source_file     = s.source_file,
               magnitude       = s.magnitude,
               magnitude_type  = s.magnitude_type,
               place           = s.place,
               event_time      = s.event_time,
               significance    = s.significance,
               network         = s.network,
               event_type      = s.event_type,
               geometry        = s.geometry
WHEN NOT MATCHED THEN
    INSERT (catalogue_month, delivered_on, source_file, event_id, magnitude,
            magnitude_type, place, event_time, significance, network,
            event_type, geometry)
    VALUES (s.catalogue_month, s.delivered_on, s.source_file, s.event_id,
            s.magnitude, s.magnitude_type, s.place, s.event_time,
            s.significance, s.network, s.event_type, s.geometry);


-- ============================================================================
-- 6. WHAT THE FIRST TWO MONTHS HOLD
-- ============================================================================

ASSERT ROW_COUNT = 2
ASSERT VALUE events = 34 WHERE catalogue_month = '2026-01'
ASSERT VALUE events = 47 WHERE catalogue_month = '2026-02'
ASSERT VALUE max_mag = 4.3 WHERE catalogue_month = '2026-01'
ASSERT VALUE max_mag = 3.7 WHERE catalogue_month = '2026-02'
ASSERT VALUE mean_mag_x100 = 283 WHERE catalogue_month = '2026-01'
ASSERT VALUE mean_mag_x100 = 284 WHERE catalogue_month = '2026-02'
SELECT catalogue_month,
       MIN(delivered_on)                                AS delivered_on,
       COUNT(*)                                         AS events,
       MAX(magnitude)                                   AS max_mag,
       CAST(ROUND(100.0 * AVG(magnitude)) AS BIGINT)    AS mean_mag_x100
FROM {{zone_name}}.seismicity.seismic_register
GROUP BY catalogue_month
ORDER BY catalogue_month;


-- ============================================================================
-- 7. THE SAME TWO PULLS AGAIN
-- ============================================================================
-- Every event matches the one already registered and updates in place, so the
-- register does not grow. A revised magnitude in a re-issued catalogue would
-- land the same way: on the event, not beside it.

MERGE INTO {{zone_name}}.seismicity.seismic_register AS t
USING (
    SELECT SUBSTRING(f.df_file_name, 23, 7)   AS catalogue_month,
           SUBSTRING(f.df_file_name, 1, 10)   AS delivered_on,
           f.df_file_name                     AS source_file,
           f.id                               AS event_id,
           f.properties_mag                   AS magnitude,
           f.properties_mag_type              AS magnitude_type,
           f.properties_place                 AS place,
           f.properties_time                  AS event_time,
           f.properties_sig                   AS significance,
           f.properties_net                   AS network,
           f.properties_type                  AS event_type,
           f.geometry
    FROM {{zone_name}}.seismicity.seismic_feed f
    WHERE f.df_file_name LIKE '2026-03-11%'
) AS s
ON t.event_id = s.event_id
WHEN MATCHED THEN
    UPDATE SET catalogue_month = s.catalogue_month,
               delivered_on    = s.delivered_on,
               source_file     = s.source_file,
               magnitude       = s.magnitude,
               magnitude_type  = s.magnitude_type,
               place           = s.place,
               event_time      = s.event_time,
               significance    = s.significance,
               network         = s.network,
               event_type      = s.event_type,
               geometry        = s.geometry
WHEN NOT MATCHED THEN
    INSERT (catalogue_month, delivered_on, source_file, event_id, magnitude,
            magnitude_type, place, event_time, significance, network,
            event_type, geometry)
    VALUES (s.catalogue_month, s.delivered_on, s.source_file, s.event_id,
            s.magnitude, s.magnitude_type, s.place, s.event_time,
            s.significance, s.network, s.event_type, s.geometry);


-- ============================================================================
-- 8. THE RE-PULL ADDED NOTHING
-- ============================================================================

ASSERT ROW_COUNT = 1
ASSERT VALUE events = 81
SELECT COUNT(*) AS events
FROM {{zone_name}}.seismicity.seismic_register;


-- ============================================================================
-- 9. LOAD MARCH
-- ============================================================================

MERGE INTO {{zone_name}}.seismicity.seismic_register AS t
USING (
    SELECT SUBSTRING(f.df_file_name, 23, 7)   AS catalogue_month,
           SUBSTRING(f.df_file_name, 1, 10)   AS delivered_on,
           f.df_file_name                     AS source_file,
           f.id                               AS event_id,
           f.properties_mag                   AS magnitude,
           f.properties_mag_type              AS magnitude_type,
           f.properties_place                 AS place,
           f.properties_time                  AS event_time,
           f.properties_sig                   AS significance,
           f.properties_net                   AS network,
           f.properties_type                  AS event_type,
           f.geometry
    FROM {{zone_name}}.seismicity.seismic_feed f
    WHERE f.df_file_name LIKE '2026-03-12%'
) AS s
ON t.event_id = s.event_id
WHEN MATCHED THEN
    UPDATE SET catalogue_month = s.catalogue_month,
               delivered_on    = s.delivered_on,
               source_file     = s.source_file,
               magnitude       = s.magnitude,
               magnitude_type  = s.magnitude_type,
               place           = s.place,
               event_time      = s.event_time,
               significance    = s.significance,
               network         = s.network,
               event_type      = s.event_type,
               geometry        = s.geometry
WHEN NOT MATCHED THEN
    INSERT (catalogue_month, delivered_on, source_file, event_id, magnitude,
            magnitude_type, place, event_time, significance, network,
            event_type, geometry)
    VALUES (s.catalogue_month, s.delivered_on, s.source_file, s.event_id,
            s.magnitude, s.magnitude_type, s.place, s.event_time,
            s.significance, s.network, s.event_type, s.geometry);


-- ============================================================================
-- 10. THE SEISMICITY IS RISING
-- ============================================================================
-- The reason the register accumulates instead of being replaced. Three months
-- and the count goes 34, 47, 64.

ASSERT ROW_COUNT = 3
ASSERT VALUE events = 34 WHERE catalogue_month = '2026-01'
ASSERT VALUE events = 47 WHERE catalogue_month = '2026-02'
ASSERT VALUE events = 64 WHERE catalogue_month = '2026-03'
ASSERT VALUE max_mag = 3.9 WHERE catalogue_month = '2026-03'
ASSERT VALUE mean_mag_x100 = 287 WHERE catalogue_month = '2026-03'
SELECT catalogue_month,
       COUNT(*)                                         AS events,
       MAX(magnitude)                                   AS max_mag,
       CAST(ROUND(100.0 * AVG(magnitude)) AS BIGINT)    AS mean_mag_x100
FROM {{zone_name}}.seismicity.seismic_register
GROUP BY catalogue_month
ORDER BY catalogue_month;


-- ============================================================================
-- 11. WHICH NETWORK RECORDED THEM
-- ============================================================================
-- The Texas and Oklahoma regional networks between them hold 137 of the 145,
-- which is what a disposal-driven sequence looks like: the events are where
-- the injection is, not spread evenly.

ASSERT ROW_COUNT = 3
ASSERT VALUE events = 117 WHERE network = 'tx'
ASSERT VALUE events = 20 WHERE network = 'ok'
ASSERT VALUE events = 8 WHERE network = 'us'
SELECT network,
       COUNT(*)         AS events,
       MAX(magnitude)   AS max_mag
FROM {{zone_name}}.seismicity.seismic_register
GROUP BY network
ORDER BY events DESC;


-- ============================================================================
-- 12. THE MAGNITUDE SCALES ARE NOT ALL THE SAME
-- ============================================================================
-- A real catalogue mixes them. 141 events are local magnitude, three are
-- revised moment magnitude and one is Lg-wave body magnitude. Averaging across
-- scales is defensible at this range and worth knowing you are doing.

ASSERT ROW_COUNT = 3
ASSERT VALUE events = 141 WHERE magnitude_type = 'ml'
ASSERT VALUE events = 3 WHERE magnitude_type = 'mwr'
ASSERT VALUE events = 1 WHERE magnitude_type = 'mb_lg'
SELECT magnitude_type,
       COUNT(*)         AS events,
       MIN(magnitude)   AS min_mag,
       MAX(magnitude)   AS max_mag
FROM {{zone_name}}.seismicity.seismic_register
GROUP BY magnitude_type
ORDER BY events DESC;


-- ============================================================================
-- 13. THE EVENTS A REGULATOR ACTS ON
-- ============================================================================
-- 41 events at magnitude 3.0 or above, and exactly one at 4.0 or above. The
-- 4.3 is the largest in the three months and the one that would be named in a
-- rate-reduction order.

ASSERT ROW_COUNT = 1
ASSERT VALUE events = 145
ASSERT VALUE at_least_3 = 41
ASSERT VALUE at_least_4 = 1
ASSERT VALUE smallest = 2.5
ASSERT VALUE largest = 4.3
ASSERT VALUE max_significance = 308
SELECT COUNT(*)                                     AS events,
       COUNT(*) FILTER (WHERE magnitude >= 3.0)     AS at_least_3,
       COUNT(*) FILTER (WHERE magnitude >= 4.0)     AS at_least_4,
       MIN(magnitude)                               AS smallest,
       MAX(magnitude)                               AS largest,
       MAX(significance)                            AS max_significance
FROM {{zone_name}}.seismicity.seismic_register;


-- ============================================================================
-- 14. EVERY EVENT IS AN EARTHQUAKE, AND EVERY ID IS DISTINCT
-- ============================================================================
-- The catalogue also carries quarry blasts and explosions, which a seismicity
-- register must not treat as induced events. None are in this window. The
-- distinct id count matching the row count is what proves the three pulls did
-- not overlap.

ASSERT ROW_COUNT = 1
ASSERT VALUE events = 145
ASSERT VALUE earthquakes = 145
ASSERT VALUE distinct_events = 145
ASSERT VALUE distinct_files = 3
SELECT COUNT(*)                                            AS events,
       COUNT(*) FILTER (WHERE event_type = 'earthquake')   AS earthquakes,
       COUNT(DISTINCT event_id)                            AS distinct_events,
       COUNT(DISTINCT source_file)                         AS distinct_files
FROM {{zone_name}}.seismicity.seismic_register;


-- ============================================================================
-- 15. EVERY PULL LOADED EXACTLY ONCE
-- ============================================================================

ASSERT ROW_COUNT = 3
ASSERT VALUE events = 34 WHERE source_file = '2026-03-11_seismicity_2026-01.geojson'
ASSERT VALUE events = 47 WHERE source_file = '2026-03-11_seismicity_2026-02.geojson'
ASSERT VALUE events = 64 WHERE source_file = '2026-03-12_seismicity_2026-03.geojson'
SELECT source_file,
       COUNT(*)                     AS events,
       COUNT(DISTINCT event_id)     AS distinct_events
FROM {{zone_name}}.seismicity.seismic_register
GROUP BY source_file
ORDER BY source_file;


-- ============================================================================
-- 16. THE STATE AFTER THE FIRST DELIVERY, BY TIME TRAVEL
-- ============================================================================

ASSERT ROW_COUNT = 81
SELECT *
FROM {{zone_name}}.seismicity.seismic_register VERSION AS OF 1;


-- ============================================================================
-- 17. THE LOAD HISTORY
-- ============================================================================

ASSERT ROW_COUNT > 0
DESCRIBE HISTORY {{zone_name}}.seismicity.seismic_register;


-- ============================================================================
-- VERIFY: All Checks
-- ============================================================================
-- The register as a monitoring team would report it: three months, 145 events,
-- rising, and the largest at 4.3.

ASSERT ROW_COUNT = 3
ASSERT VALUE events = 34 WHERE catalogue_month = '2026-01'
ASSERT VALUE events = 47 WHERE catalogue_month = '2026-02'
ASSERT VALUE events = 64 WHERE catalogue_month = '2026-03'
ASSERT VALUE networks = 3 WHERE catalogue_month = '2026-01'
ASSERT VALUE with_geometry = 34 WHERE catalogue_month = '2026-01'
ASSERT VALUE with_geometry = 47 WHERE catalogue_month = '2026-02'
ASSERT VALUE with_geometry = 64 WHERE catalogue_month = '2026-03'
ASSERT VALUE largest = 4.3 WHERE catalogue_month = '2026-01'
SELECT catalogue_month,
       MIN(delivered_on)                AS delivered_on,
       COUNT(*)                         AS events,
       COUNT(DISTINCT network)          AS networks,
       COUNT(geometry)                  AS with_geometry,
       MAX(magnitude)                   AS largest
FROM {{zone_name}}.seismicity.seismic_register
GROUP BY catalogue_month
ORDER BY catalogue_month;
