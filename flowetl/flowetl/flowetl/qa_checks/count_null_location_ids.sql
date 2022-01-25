SELECT
    count(*)
FROM
    {{ final_table }}
WHERE
    location_id IS NULL
