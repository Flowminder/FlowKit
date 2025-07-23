SELECT
    count(*)
FROM (
    SELECT
        imei
    FROM
        {{ final_table }}
    WHERE imei IS NOT NULL
    GROUP BY
        imei) _
