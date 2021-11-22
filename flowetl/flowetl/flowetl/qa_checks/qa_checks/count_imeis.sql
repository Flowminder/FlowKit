SELECT
    count(*)
FROM (
    SELECT
        imei
    FROM
        {{ final_table }}
    GROUP BY
        imei) _
