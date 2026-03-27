SELECT
    count(*)
FROM (
    SELECT
        imsi
    FROM
        {{ final_table }}
    WHERE imsi IS NOT NULL
    GROUP BY
        imsi) _
