DROP TABLE IF EXISTS {{ transform_table }};
CREATE TABLE {{ transform_table }} AS (
    SELECT
        *
    FROM
        {{ extract_table }}
);

ALTER TABLE {{transform_table}}
    ALTER COLUMN msisdn SET NOT NULL,
    ALTER COLUMN datetime SET NOT NULL;
