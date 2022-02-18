/*
Drops every row from staging_table containing an msisdn from
the list at opt_out_path.
Quary args:
-date
-opt_out_path
*/

BEGIN;

CREATE TEMPORARY TABLE to_be_removed(
    msisdn varchar(32)
);

COPY to_be_removed FROM '{{opt_out_path}}' (FORMAT CSV, HEADER TRUE);

DELETE FROM staging_table_{{ds_nodash}}
    WHERE msisdn IN (SELECT msisdn FROM to_be_removed);

COMMIT;