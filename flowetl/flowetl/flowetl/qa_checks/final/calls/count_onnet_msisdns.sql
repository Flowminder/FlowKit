SELECT
    count(*)
FROM (
    SELECT
        msisdn
    FROM
        {{ final_table }}
    GROUP BY
        msisdn) _
