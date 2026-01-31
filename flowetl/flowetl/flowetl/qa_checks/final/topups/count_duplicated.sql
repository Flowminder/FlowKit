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
        type,
        recharge_amount,
        airtime_fee,
        tax_and_fee,
        pre_event_balance,
        post_event_balance,
        operator_code,
        country_code
    HAVING count(*) > 1) tableWithCount