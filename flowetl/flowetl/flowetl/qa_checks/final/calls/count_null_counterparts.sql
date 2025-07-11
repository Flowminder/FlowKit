SELECT
    count(*)
FROM
    {{ final_table }}
WHERE
    msisdn_counterpart IS NULL
