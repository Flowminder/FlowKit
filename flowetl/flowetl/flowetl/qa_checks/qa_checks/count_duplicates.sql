SELECT COALESCE(sum(n_dupes), 0) FROM
  (SELECT count(*) - 1 as n_dupes
    FROM {{ final_table }}
    GROUP BY
    {% if cdr_type == 'calls' %}
        outgoing,
        duration,
        msisdn_counterpart,
        network,
    {% elif cdr_type == 'sms' %}
        outgoing,
        msisdn_counterpart,
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
        datetime,
        msisdn,
        location_id,
        imsi,
        imei,
        tac,
        operator_code,
        country_code
    HAVING count(*) > 1) tableWithCount