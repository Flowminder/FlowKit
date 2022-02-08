BEGIN;

-- TODO: Move to flowdb
CREATE SCHEMA IF NOT EXISTS reduced;

DROP TABLE IF EXISTS reduced.sightings_{{params.date}};

--TODO: msisdn and locationid to bytea from text
CREATE TABLE reduced.sightings_{{params.date}}(
 LIKE reduced.sightings,
 CONSTRAINT sightings_{{params.date}}_check
    CHECK (sighting_date >= DATE '{{params.date}}' AND sighting_date < DATE '{{params.date}}' + 1) -- Switch to range
);

WITH ranked AS (
    SELECT date_time, cell_id, msisdn, event_type,
           dense_rank() OVER ( PARTITION BY msisdn, cell_id
                               ORDER BY date_time
                             ) AS cell_id_rank,
           dense_rank() OVER ( PARTITION BY msisdn
                               ORDER BY date_time
                             ) AS date_rank
    FROM staging_table_{{params.date}}
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
INSERT INTO reduced.sightings_{{params.date}}
    SELECT '{{params.date}}', convert_to(msisdn, 'LATIN1'), row_id, cell_id, cons_dates, cons_events
    FROM aggregated;

-- Clustering needs indexing; I think we leave it out for the time being
--CLUSTER reduced.sightings_{{params.date}}
--USING subscriber_id, row_id;

ANALYZE reduced.sightings_{{params.date}};

ALTER TABLE reduced.sightings
ATTACH PARTITION reduced.sightings_{{params.date}} FOR VALUES FROM (date('{{params.date}}')) TO (date('{{params.date}}')+ 1) ;

ALTER TABLE reduced.sightings_{{params.date}}
DROP CONSTRAINT sightings_{{params.date}}_check;

ANALYZE reduced.sightings;

--TODO: Use CHECK clause for perf (see notebook)

COMMIT;