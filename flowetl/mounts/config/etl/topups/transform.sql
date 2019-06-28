DROP TABLE IF EXISTS {{ dag_run.conf.transform_table }};
CREATE TABLE {{ dag_run.conf.transform_table }} AS (
    SELECT
        *
    FROM
        {{ dag_run.conf.extract_table }}
);

ALTER TABLE {{dag_run.conf.transform_table}}
    ALTER COLUMN msisdn SET NOT NULL,
    ALTER COLUMN datetime SET NOT NULL;
