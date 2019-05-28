DROP TABLE IF EXISTS {{ transform_table }};
CREATE TABLE {{ transform_table }} AS (
    SELECT
        *
    FROM
        {{ extract_table }}
)
