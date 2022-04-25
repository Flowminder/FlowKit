BEGIN;

DROP TABLE IF EXISTS etl.staging_table_{{ds_nodash}};
CREATE TABLE etl.staging_table_{{ds_nodash}} AS (
    {% set union = joiner('\nUNION ALL\n') %}
    {% for event in params.event_types %}
    {{ union() }}
    SELECT
		CAST (MSISDN AS bytea) AS MSISDN,
		CAST (IMEI AS bytea) AS IMEI,
		CAST (IMSI AS bytea) AS IMSI,
		CAST (TAC AS bytea) AS TAC,
		CAST (CELL_ID AS  bytea) AS CELL_ID,
		CAST (DATE_TIME AS timestamptz) AS DATE_TIME,
		CAST (EVENT_TYPE AS int) AS EVENT_TYPE
	FROM {{event}}_table_{{ds_nodash}}
	{% endfor %}
	ORDER BY date_time
    );
COMMIT;