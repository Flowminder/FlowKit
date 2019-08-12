-- Create a foreign table wrapping the external CSV file with the columns msisdn, event_time, cell_id.
CREATE FOREIGN TABLE {{ get_extract_view(ds_nodash) }} (
  msisdn text,
  event_time timestamp with time zone,
  cell_id text
) SERVER csv_fdw
OPTIONS ( filename '{{ dag_run.conf["full_file_path"] }}', format 'csv', header 'true' );
