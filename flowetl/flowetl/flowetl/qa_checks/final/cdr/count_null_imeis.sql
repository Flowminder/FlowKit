SELECT
    count(*)
FROM
    {{ final_table }}
WHERE
    imei IS NULL
