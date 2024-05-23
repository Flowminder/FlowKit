SELECT
    max(msisdn_count)
FROM (
    SELECT
        imei,
        count(*) AS msisdn_count
    FROM (
        SELECT
            imei,
            msisdn
        FROM
            {{ final_table }}
        WHERE imei IS NOT NULL
        GROUP BY
            imei,
            msisdn) _
    GROUP BY
        imei) _
