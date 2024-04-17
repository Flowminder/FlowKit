SELECT count(*) FROM (SELECT distinct msisdn FROM
      (SELECT msisdn
      FROM
        {{ final_table }}
        {% if cdr_type == "calls" or cdr_type == "sms" %}
        UNION ALL
        (SELECT msisdn_counterpart AS msisdn FROM {{ final_table }} WHERE msisdn_counterpart NOTNULL)
        {% endif %}
        ) _
    ) _