BEGIN;

-- Can probably put this elsewhere, but it can live here while developing
CREATE SERVER IF NOT EXISTS csv_fdw FOREIGN DATA WRAPPER file_fdw;

DROP FOREIGN TABLE IF EXISTS call_table_{{ds_nodash}};
CREATE FOREIGN TABLE call_table_{{ds_nodash}}(
	MSISDN text,
	IMEI text,
	IMSI text,
	TAC text,
	CELL_ID text,
	DATE_TIME timestamptz,
	EVENT_ID int,
	EVENT_TYPE smallint,
	IS_INCOMING bool,
	OTHER_MSISDN text,
	DURATION real
) SERVER csv_fdw
OPTIONS (filename '{{params.flowdb_csv_dir}}/{{ds_nodash}}_call.csv', format 'csv', header 'TRUE');

DROP FOREIGN TABLE IF EXISTS sms_table_{{ds_nodash}};
CREATE FOREIGN TABLE sms_table_{{ds_nodash}}(
	MSISDN text,
	IMEI text,
	IMSI text,
	TAC text,
	CELL_ID text,
	DATE_TIME timestamptz,
	EVENT_ID int,
	EVENT_TYPE smallint,
	IS_INCOMING bool,
	OTHER_MSISDN text
) SERVER csv_fdw
OPTIONS (filename '{{params.flowdb_csv_dir}}/{{ds_nodash}}_sms.csv', format 'csv', header 'TRUE');

DROP FOREIGN TABLE IF EXISTS location_table_{{ds_nodash}};
CREATE FOREIGN TABLE location_table_{{ds_nodash}}(
	MSISDN text,
	IMEI text,
	IMSI text,
	TAC text,
	CELL_ID text,
	DATE_TIME timestamptz,
	EVENT_ID int,
	EVENT_TYPE smallint
) SERVER csv_fdw
OPTIONS (filename '{{params.flowdb_csv_dir}}/{{ds_nodash}}_location.csv', format 'csv', header 'TRUE');

DROP FOREIGN TABLE IF EXISTS mds_table_{{ds_nodash}};
CREATE FOREIGN TABLE mds_table_{{ds_nodash}}(
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
OPTIONS (filename '{{params.flowdb_csv_dir}}/{{ds_nodash}}_mds.csv', format 'csv', header 'TRUE');

DROP FOREIGN TABLE IF EXISTS topup_table_{{ds_nodash}};
CREATE FOREIGN TABLE topup_table_{{ds_nodash}}(
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
OPTIONS (filename '{{params.flowdb_csv_dir}}/{{ds_nodash}}_topup.csv', format 'csv', header 'TRUE');

COMMIT;