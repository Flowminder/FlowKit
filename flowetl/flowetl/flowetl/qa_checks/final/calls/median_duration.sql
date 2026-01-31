SELECT
    percentile_cont(0.5) WITHIN GROUP (ORDER BY duration)
FROM
    {{ final_table }}