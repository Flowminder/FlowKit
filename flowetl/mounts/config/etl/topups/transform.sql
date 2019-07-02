DROP TABLE IF EXISTS {{ get_transform_table(ds_nodash) }};
CREATE TABLE {{ get_transform_table(ds_nodash) }} AS (
    SELECT
        *
    FROM
        {{ get_extract_view(ds_nodash) }}
);

ALTER TABLE {{get_transform_table(ds_nodash)}}
    ALTER COLUMN msisdn SET NOT NULL,
    ALTER COLUMN datetime SET NOT NULL;
