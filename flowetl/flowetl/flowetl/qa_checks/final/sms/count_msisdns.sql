SELECT count(*) FROM (SELECT distinct msisdn FROM
      (SELECT msisdn
      FROM
        {{ final_table }}
        UNION ALL
        (SELECT msisdn_counterpart AS msisdn FROM {{ final_table }} WHERE msisdn_counterpart NOTNULL)
        ) _
    ) _