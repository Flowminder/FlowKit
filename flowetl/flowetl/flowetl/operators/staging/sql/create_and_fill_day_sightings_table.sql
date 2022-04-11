BEGIN;

DROP TABLE IF EXISTS reduced.sightings_{{ds_nodash}};

CREATE TABLE reduced.sightings_{{ds_nodash}} (
    sighting_date,
    sub_id,
    sighting_id,
    location_id,
    event_times,
    event_types
)AS (
    SELECT DATE '{{ds_nodash}}', convert_to(msisdn, 'LATIN1'), CAST (row_id AS INT), cell_id, cons_times, cons_events
    FROM (
        SELECT msisdn,
           row_number() OVER ( PARTITION BY msisdn
                               ORDER BY min(date_time), max(date_time)
                             ) AS row_id,
           cell_id,
           array_agg(date_time ORDER BY date_time) AS cons_times,
           array_agg(event_type ORDER BY date_time) AS cons_events
        FROM (
           SELECT
                date_time,
                cell_id,
                msisdn,
                cell_id_rank,
                date_rank,
                event_type,
                cell_id_rank - date_rank AS group_rank
            FROM (
                SELECT
                     date_time,
                     cell_id,
                     msisdn,
                     event_type,
                     dense_rank() OVER ( PARTITION BY msisdn, cell_id
                                         ORDER BY date_time
                                       ) AS cell_id_rank,
                     dense_rank() OVER ( PARTITION BY msisdn
                                         ORDER BY date_time
                                       ) AS date_rank
                FROM etl.staging_table_{{ds_nodash}}
                ORDER BY date_time
            ) AS ranked
            ORDER BY date_time
        ) AS group_ranked
        GROUP BY msisdn, cell_id, group_rank
        ORDER BY msisdn, row_id
    ) AS aggregated
);

ALTER TABLE reduced.sightings_{{ds_nodash}}
    ALTER COLUMN sub_id SET NOT NULL,
    ALTER COLUMN sighting_id SET NOT NULL,
    ADD CONSTRAINT sightings_{{ds_nodash}}_check
        CHECK (sighting_date >= DATE '{{ds_nodash}}' AND sighting_date < DATE '{{ds_nodash}}' + 1);


-- Clustering needs indexing; I think we leave it out for the time being
--CLUSTER reduced.sightings_{{ds_nodash}}
--USING subscriber_id, row_id;


--TODO: Use CHECK clause for perf (see notebook)

COMMIT;