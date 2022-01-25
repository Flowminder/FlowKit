SELECT
    count(*)
FROM (
    SELECT
        imsi
    FROM
        {{ final_table }}
    GROUP BY
        imsi) _
