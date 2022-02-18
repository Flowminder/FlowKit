BEGIN;

-- TODO: Need some state holding

DROP TABLE IF EXISTS reduced.cell_location_mapping;
CREATE TABLE reduced.cell_location_mapping(
    cell_id text,
    location_id text
);

-- A one-to-one mapping
WITH cells AS (
    SELECT DISTINCT cell_id FROM staging_table_{{ds_nodash}}
)
INSERT INTO reduced.cell_location_mapping(
    SELECT
        cell_id, cell_id
    FROM cells
);

COMMIT;