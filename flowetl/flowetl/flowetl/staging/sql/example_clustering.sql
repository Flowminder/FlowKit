BEGIN;

UPDATE staging_table_{date}
    SET cell_id = "A" WHERE cell_id = "11111" OR cell_id = "22222"
    SET cell_id = "B" WHERE cell_id = "33333" OR cell_id = "44444"
    SET cell_id = "C" WHERE cell_id = "55555" OR cell_id = "66666"

COMMIT;