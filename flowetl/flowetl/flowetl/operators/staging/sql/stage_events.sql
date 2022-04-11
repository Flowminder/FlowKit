BEGIN;

DROP TABLE IF EXISTS staging.staging_table_{{ds_nodash}};
CREATE TABLE staging.staging_table_{{ds_nodash}} AS (
    {% set union = joiner('\nUNION ALL\n') %}
    {% for event in params.event_types %}
    {{ union() }}
    SELECT
		MSISDN,
		IMEI,
		IMSI,
		TAC,
		CELL_ID,
		DATE_TIME,
		EVENT_TYPE
	FROM call_table_{{ds_nodash}}
	{% endfor %}
	ORDER BY date_time
    );
COMMIT;