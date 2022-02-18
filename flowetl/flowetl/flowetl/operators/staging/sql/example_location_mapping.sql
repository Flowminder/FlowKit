BEGIN;

-- Table is a many-to-one mapping between cell IDs and location IDs
DROP TABLE IF EXISTS reduced.cell_location_mapping;
CREATE TABLE reduced.cell_location_mapping(
    cell_id text,
    location_id text
);

-- Can we drop this at this stage? We don't need the cell IDs anymore.

WITH cells AS (
    SELECT DISTINCT cell_id FROM staging_table_{{ds_nodash}} AS cell_id
)
INSERT INTO reduced.cell_location_mapping(
    -- In practice, this would be a far more complex cell-clustering algorithm
    SELECT cell_id,
         CASE WHEN cell_id = '11111' OR cell_id = '22222' THEN 'A'
         WHEN cell_id = '33333' OR cell_id = '44444' THEN 'B'
         WHEN cell_id = '55555' OR cell_id = '66666' THEN 'C'
         END AS location_id
    FROM cells
);

COMMIT;