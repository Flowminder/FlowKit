SELECT
    count(*)
FROM
    {{ final_table }}
WHERE
    imsi IS NULL
