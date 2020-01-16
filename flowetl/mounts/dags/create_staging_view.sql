SELECT event_time, msisdn, cell_id FROM {{ source_table }}
WHERE event_time >= '{{ ds_nodash }}' AND event_time < '{{ tomorrow_ds_nodash }}';