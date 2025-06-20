SELECT COALESCE(sum(n_dupes), 0) FROM
  (SELECT count(*) - 1 as n_dupes
    FROM {{ final_table }}
    GROUP BY
        msisdn,
        datetime,
        imsi,
        imei,
        tac,
        location_id,
    {% if cdr_type == 'calls' %}
        msisdn_counterpart,
        outgoing,
        duration,
        network,
    {% elif cdr_type == 'sms' %}
        msisdn_counterpart,
        outgoing,
        network,
    {% elif cdr_type == 'mds' %}
        duration,
        volume_total,
        volume_upload,
        volume_download,
    {% elif cdr_type == 'topups' %}
        type,
        recharge_amount,
        airtime_fee,
        tax_and_fee,
        pre_event_balance,
        post_event_balance,
    {% endif %}
        operator_code,
        country_code
    HAVING count(*) > 1) tableWithCount