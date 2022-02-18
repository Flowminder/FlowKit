BEGIN;

UPDATE staging_table_{{ds_nodash}}
    SET cell_id =
        (SELECT location_id
        FROM reduced.cell_location_mapping
        WHERE reduced.cell_location_mapping.cell_id = staging_table_{{ds_nodash}}.cell_id);

COMMIT;