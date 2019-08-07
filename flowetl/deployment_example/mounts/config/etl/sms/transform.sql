DROP TABLE IF EXISTS {{ get_transform_table(ds_nodash) }};
CREATE TABLE {{ get_transform_table(ds_nodash) }} (LIKE events.{{ cdr_type }} INCLUDING ALL);

INSERT INTO {{ get_transform_table(ds_nodash) }} (msisdn, datetime, location_id)
    SELECT msisdn, event_time, cell_id
    FROM {{ get_extract_view(ds_nodash) }}
;

ALTER TABLE {{get_transform_table(ds_nodash)}}
    ALTER COLUMN msisdn SET NOT NULL,
    ALTER COLUMN datetime SET NOT NULL;
