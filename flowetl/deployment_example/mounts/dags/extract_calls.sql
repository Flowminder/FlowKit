SELECT uuid_generate_v4()::TEXT as id, TRUE as outgoing, event_time::TIMESTAMPTZ as datetime, NULL::NUMERIC as duration,
    NULL::TEXT as network, msisdn::TEXT COLLATE "C" as msisdn, NULL::TEXT COLLATE "C" as msisdn_counterpart, cell_id::TEXT as location_id, NULL::TEXT COLLATE "C" as imsi,
    NULL::TEXT COLLATE "C" as imei, NULL::NUMERIC(8) as tac, NULL::NUMERIC as operator_code, NULL::NUMERIC as country_code

    FROM {{ staging_table }}