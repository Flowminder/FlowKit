INSERT INTO events.sample (event_time, msisdn, cell_id)
SELECT
    -- Note: we subtract 1 month from the current date to ensure that all
    --       dates are in the past (otherwise Airflow won't immediately
    --       execute the ingestion tasks).
    now() - INTERVAL '1 month' + i * INTERVAL '1 second' as event_time,
    md5(i::text) || md5(i::text) as msisdn,
    to_char(random() * 100000, '00000') as cell_id
FROM generate_series(0,500000) s(i);
