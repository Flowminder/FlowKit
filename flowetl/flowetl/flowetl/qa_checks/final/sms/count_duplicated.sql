SELECT count(*) FROM
  (SELECT count(*) as n_dupes
    FROM {{ final_table }}
    GROUP BY
        msisdn,
        datetime,
        imsi,
        imei,
        tac,
        location_id,
        msisdn_counterpart,
        outgoing,
        network,
        operator_code,
        country_code
    HAVING count(*) > 1) tableWithCount