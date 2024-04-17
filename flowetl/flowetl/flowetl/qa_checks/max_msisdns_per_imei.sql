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
        GROUP BY
            imei,
            msisdn) _
    GROUP BY
        imei) _
