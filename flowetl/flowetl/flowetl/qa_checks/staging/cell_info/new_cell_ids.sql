SELECT
    count(*)
FROM {{ staging_table }} LEFT OUTER JOIN infrastructure.cells
    USING (id)
WHERE version ISNULL