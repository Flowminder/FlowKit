SELECT count(*) FROM
  (SELECT DISTINCT location_id
  FROM
   {{ final_table }}
    ) _