BEGIN;

UPDATE staging_table_{date}
    SET cell_id =
        CASE WHEN cell_id = '11111' OR cell_id = '22222' THEN 'A'
         WHEN cell_id = '33333' OR cell_id = '44444' THEN 'B'
         WHEN cell_id = '55555' OR cell_id = '66666' THEN 'C'
        END;
COMMIT;