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
        duration,
        volume_total,
        volume_upload,
        volume_download,
        operator_code,
        country_code
    HAVING count(*) > 1) tableWithCount