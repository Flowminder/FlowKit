SELECT
    count(*)
FROM
    {{ final_table }}
WHERE
    outgoing
