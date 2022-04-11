BEGIN;
--
-- TODO: CSV-ise the event type enum + add loading query




DROP TABLE IF EXISTS staging.staging_table_{{ds_nodash}};
CREATE TABLE staging.staging_table_{{ds_nodash}} AS(
	SELECT
		MSISDN,
		IMEI,
		IMSI,
		TAC,
		CELL_ID,
		DATE_TIME,
		EVENT_TYPE
	FROM call_table_{{ds_nodash}}
	UNION ALL
	SELECT
		MSISDN,
		IMEI,
		IMSI,
		TAC,
		CELL_ID,
		DATE_TIME,
		EVENT_TYPE
	FROM location_table_{{ds_nodash}}
	UNION ALL
	SELECT
		MSISDN,
		IMEI,
		IMSI,
		TAC,
		CELL_ID,
		DATE_TIME,
		EVENT_TYPE
	FROM sms_table_{{ds_nodash}}
	UNION ALL
	SELECT
		MSISDN,
		IMEI,
		IMSI,
		TAC,
		CELL_ID,
		DATE_TIME,
		EVENT_TYPE
	FROM mds_table_{{ds_nodash}}
	UNION ALL
	SELECT
		MSISDN,
		IMEI,
		IMSI,
		TAC,
		CELL_ID,
		DATE_TIME,
		EVENT_TYPE
	FROM topup_table_{{ds_nodash}}
	ORDER BY date_time);
COMMIT;