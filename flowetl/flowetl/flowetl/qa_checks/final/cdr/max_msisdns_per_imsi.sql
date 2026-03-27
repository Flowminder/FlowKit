SELECT
    max(msisdn_count)
FROM (
    SELECT
        imsi,
        count(*) AS msisdn_count
    FROM (
        SELECT
            imsi,
            msisdn
        FROM
            {{ final_table }}
        WHERE imsi IS NOT NULL
        GROUP BY
            imsi,
            msisdn) _
    GROUP BY
        imsi) _
