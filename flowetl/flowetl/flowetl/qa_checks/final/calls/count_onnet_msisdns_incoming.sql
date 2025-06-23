SELECT
    count(*)
FROM (
    SELECT
        msisdn
    FROM
        {{ final_table }}
    WHERE
        NOT outgoing
    GROUP BY
        msisdn) _
