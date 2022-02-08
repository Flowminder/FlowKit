BEGIN;



-- TODO: CSV-ise the event type enum + add loading query

-- Can probably put this elsewhere, but it can live here while developing
CREATE SERVER IF NOT EXISTS csv_fdw FOREIGN DATA WRAPPER file_fdw;

DROP FOREIGN TABLE IF EXISTS call_table_{{params.date}};
CREATE FOREIGN TABLE call_table_{{params.date}}(
	MSISDN text,
	IMEI text,
	IMSI text,
	TAC text,
	CELL_ID text,
	DATE_TIME timestamptz,
	EVENT_ID int,
	EVENT_TYPE smallint,
	OTHER_MSISDN text,
	DURATION real
) SERVER csv_fdw
OPTIONS (filename '{{params.csv_dir}}/{{params.date}}_call.csv', format 'csv', header 'TRUE');

DROP FOREIGN TABLE IF EXISTS sms_table_{{params.date}};
CREATE FOREIGN TABLE sms_table_{{params.date}}(
	MSISDN text,
	IMEI text,
	IMSI text,
	TAC text,
	CELL_ID text,
	DATE_TIME timestamptz,
	EVENT_ID int,
	EVENT_TYPE smallint,
	OTHER_MSISDN text
) SERVER csv_fdw
OPTIONS (filename '{{params.csv_dir}}/{{params.date}}_sms.csv', format 'csv', header 'TRUE');

DROP FOREIGN TABLE IF EXISTS location_table_{{params.date}};
CREATE FOREIGN TABLE location_table_{{params.date}}(
	MSISDN text,
	IMEI text,
	IMSI text,
	TAC text,
	CELL_ID text,
	DATE_TIME timestamptz,
	EVENT_ID int,
	EVENT_TYPE smallint
) SERVER csv_fdw
OPTIONS (filename '{{params.csv_dir}}/{{params.date}}_location.csv', format 'csv', header 'TRUE');

DROP FOREIGN TABLE IF EXISTS mds_table_{{params.date}};
CREATE FOREIGN TABLE mds_table_{{params.date}}(
	MSISDN text,
	IMEI text,
	IMSI text,
	TAC text,
	CELL_ID text,
	DATE_TIME timestamptz,
	EVENT_ID int,
	EVENT_TYPE smallint,
	DATA_VOLUME_UP int,
	DATA_VOLUME_DOWN int,
	DURATION real
) SERVER csv_fdw
OPTIONS (filename '{{params.csv_dir}}/{{params.date}}_mds.csv', format 'csv', header 'TRUE');

DROP FOREIGN TABLE IF EXISTS topup_table_{{params.date}};
CREATE FOREIGN TABLE topup_table_{{params.date}}(
	MSISDN text,
	IMEI text,
	IMSI text,
	TAC text,
	CELL_ID text,
	DATE_TIME timestamptz,
	EVENT_ID int,
	EVENT_TYPE smallint,
	RECHARGE_AMOUNT real,
	AIRTIME_FEE real,
	TAX_AND_FEE real,
	PRE_EVENT_BALANCE real,
	POST_EVENT_BALANCE real
) SERVER csv_fdw
OPTIONS (filename '{{params.csv_dir}}/{{params.date}}_topup.csv', format 'csv', header 'TRUE');

DROP TABLE IF EXISTS staging_table_{{params.date}};
CREATE TABLE staging_table_{{params.date}} AS(
	SELECT
		MSISDN,
		IMEI,
		IMSI,
		TAC,
		CELL_ID,
		DATE_TIME,
		EVENT_TYPE
	FROM call_table_{{params.date}}
	UNION ALL
	SELECT
		MSISDN,
		IMEI,
		IMSI,
		TAC,
		CELL_ID,
		DATE_TIME,
		EVENT_TYPE
	FROM location_table_{{params.date}}
	UNION ALL
	SELECT
		MSISDN,
		IMEI,
		IMSI,
		TAC,
		CELL_ID,
		DATE_TIME,
		EVENT_TYPE
	FROM sms_table_{{params.date}}
	UNION ALL
	SELECT
		MSISDN,
		IMEI,
		IMSI,
		TAC,
		CELL_ID,
		DATE_TIME,
		EVENT_TYPE
	FROM mds_table_{{params.date}}
	UNION ALL
	SELECT
		MSISDN,
		IMEI,
		IMSI,
		TAC,
		CELL_ID,
		DATE_TIME,
		EVENT_TYPE
	FROM topup_table_{{params.date}}
	ORDER BY date_time);
COMMIT;