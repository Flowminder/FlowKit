DROP TABLE IF EXISTS {{ transform_table(ds_nodash) }};
CREATE TABLE {{ transform_table(ds_nodash) }} AS (
    SELECT
        *
    FROM
        {{ extract_table(ds_nodash) }}
);

ALTER TABLE {{transform_table(ds_nodash)}}
    ALTER COLUMN msisdn SET NOT NULL,
    ALTER COLUMN datetime SET NOT NULL;
