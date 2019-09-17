-- Create a view into the source table containing the rows for the current date to be ingested.
CREATE OR REPLACE VIEW {{ get_extract_view(ds_nodash) }} AS
SELECT
    event_time,
    msisdn,
    cell_id
FROM {{ dag_run.conf.source_table }}
WHERE event_time >= '{{ ds_nodash }}' AND event_time < '{{ tomorrow_ds_nodash }}';
