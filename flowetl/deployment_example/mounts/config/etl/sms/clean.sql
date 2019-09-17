-- Drop views/tables used during the ETL process which are no longer needed.
DROP FOREIGN TABLE IF EXISTS {{ get_extract_view(ds_nodash) }};
DROP TABLE IF EXISTS {{ get_transform_table(ds_nodash) }};
