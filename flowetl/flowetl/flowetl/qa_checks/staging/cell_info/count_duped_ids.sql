SELECT COALESCE(sum(n_dupes), 0) FROM
          (SELECT count(*) - 1 as n_dupes
            FROM {{ staging_table }}
            GROUP BY
                cell_id
            HAVING count(*) - 1 > 1) tableWithCount