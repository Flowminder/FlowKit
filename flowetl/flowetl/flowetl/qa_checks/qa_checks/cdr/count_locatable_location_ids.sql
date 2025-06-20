SELECT
    count(*)
FROM (
    SELECT
        location_id
    FROM
        {{ final_table }} ev
        INNER JOIN infrastructure.cells cl ON ev.location_id = cl.id
    WHERE
        '{{ ds }}'::date BETWEEN coalesce(cl.date_of_first_service, '-infinity'::timestamptz)
                             AND coalesce(cl.date_of_last_service, 'infinity'::timestamptz)
        AND cl.geom_point NOTNULL
    GROUP BY
        location_id) _