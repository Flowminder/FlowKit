BEGIN;

UPDATE staging_table_{date}
    SET cell_id =
        (SELECT location_id
        FROM reduced.cell_location_mapping
        WHERE reduced.cell_location_mapping.cell_id = staging_table_{date}.cell_id);

COMMIT;