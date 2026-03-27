SELECT count(*) FROM (SELECT distinct msisdn FROM
      (SELECT msisdn
      FROM
        {{ final_table }}
        ) _
    ) _