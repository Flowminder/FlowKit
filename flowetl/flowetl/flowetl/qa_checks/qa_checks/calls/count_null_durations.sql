SELECT
    count(*)
FROM
    {{ final_table }}
WHERE
    duration IS NULL
