SELECT
    count(*)
FROM {{ extract_table }} LEFT OUTER JOIN infrastructure.cells
    USING (id)
WHERE version ISNULL