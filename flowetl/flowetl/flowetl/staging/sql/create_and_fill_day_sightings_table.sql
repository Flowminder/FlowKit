BEGIN;

-- TODO: Move to flowdb
CREATE SCHEMA IF NOT EXISTS reduced;

DROP TABLE IF EXISTS reduced.sightings_{date};

--TODO: msisdn and locationid to bytea from text
CREATE TABLE reduced.sightings_{date}(
    sighting_date DATE,
    msisdn bytea NOT NULL, -- change to bytea
    sighting_id INTEGER NOT NULL,
    location_id text, --NOT NULL REFERENCES reduced.cell_location_mapping(location_id),  -- change to bytea
    event_times TIME[],
    event_types smallint[] -- change to enum (same amount of space, but probably better)
);

WITH ranked AS (
    SELECT date_time, cell_id, msisdn, event_type,
           dense_rank() OVER ( PARTITION BY msisdn, cell_id
                               ORDER BY date_time
                             ) AS cell_id_rank,
           dense_rank() OVER ( PARTITION BY msisdn
                               ORDER BY date_time
                             ) AS date_rank
    FROM staging_table_{date}
    ORDER BY date_time
), group_ranked AS (
    SELECT date_time, cell_id, msisdn, cell_id_rank, date_rank,
           event_type, cell_id_rank - date_rank AS group_rank
    FROM ranked
    ORDER BY date_time
), aggregated AS (
    SELECT msisdn,
           row_number() OVER ( PARTITION BY msisdn
                               ORDER BY min(date_time), max(date_time)
                             ) AS row_id,
           cell_id,
           array_agg(date_time ORDER BY date_time) AS cons_dates,
           array_agg(event_type ORDER BY date_time) AS cons_events
    FROM group_ranked
    GROUP BY msisdn, cell_id, group_rank
    ORDER BY msisdn, row_id
)
INSERT INTO reduced.sightings_{date}
    SELECT '{date}', convert_to(msisdn, 'LATIN1'), row_id, cell_id, cons_dates, cons_events
    FROM aggregated;


--TODO: Attach to main numberedsquashlist (or w/e we call it) as partition. Use CHECK clause for perf (see notebook)

COMMIT;