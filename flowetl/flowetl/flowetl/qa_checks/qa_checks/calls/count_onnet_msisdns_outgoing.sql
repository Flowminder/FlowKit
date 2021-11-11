SELECT
    count(*)
FROM (
    SELECT
        msisdn
    FROM
        {{ final_table }}
    WHERE
        outgoing
    GROUP BY
        msisdn) _
