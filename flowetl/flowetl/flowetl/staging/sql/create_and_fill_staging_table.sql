BEGIN;

DROP FOREIGN TABLE IF EXISTS {{{{ staging_table }}}};
CREATE FOREIGN TABLE {{{{ staging_table }}}} (
{{{{ params.fields }}}}
) SERVER csv_fdw
OPTIONS ({{% if params.program is defined %}}program {{% else %}}filename {{% endif %}} '{{% if params.program is defined %}}{{{{ params.program }}}} {{% endif %}}{filename}',
       format 'csv',
       delimiter '{{{{ params.delimiter }}}}',
       header '{{{{ params.header }}}}',
       null '{{{{ params.null }}}}',
       quote '{{{{ params.quote }}}}',
       escape '{{{{ params.escape }}}}'
       {{% if params.encoding is defined %}}, {{{{ params.encoding }}}} {{% endif %}}
       );


-- TODO:Replace these with foreign tables
-- TODO: CSV-ise the event type enum + add loading query

CREATE TEMPORARY TABLE call_table_{date}(
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
);
CREATE TEMPORARY TABLE sms_table_{date}(
	MSISDN text,
	IMEI text,
	IMSI text,
	TAC text,
	CELL_ID text,
	DATE_TIME timestamptz,
	EVENT_ID int,
	EVENT_TYPE smallint,
	OTHER_MSISDN text
);
CREATE TEMPORARY TABLE location_table_{date}(
	MSISDN text,
	IMEI text,
	IMSI text,
	TAC text,
	CELL_ID text,
	DATE_TIME timestamptz,
	EVENT_TYPE smallint
);
CREATE TEMPORARY TABLE mds_table_{date}(
	MSISDN text,
	IMEI text,
	IMSI text,
	TAC text,
	CELL_ID text,
	DATE_TIME timestamptz,
	EVENT_TYPE smallint,
	DATA_VOLUME_UP int,
	DATA_VOLUME_DOWN int,
	DURATION real
);
CREATE TEMPORARY TABLE topup_table_{date}(
	MSISDN text,
	IMEI text,
	IMSI text,
	TAC text,
	CELL_ID text,
	DATE_TIME timestamptz,
	EVENT_TYPE smallint,
	RECHARGE_AMOUNT real,
	AIRTIME_FEE real,
	TAX_AND_FEE real,
	PRE_EVENT_BALANCE real,
	POST_EVENT_BALANCE real
);

COPY call_table_{date} FROM '{csv_dir}/{date}_calls.csv' (FORMAT CSV, HEADER TRUE);
COPY location_table_{date} FROM '{csv_dir}/{date}_locations.csv' (FORMAT CSV, HEADER TRUE);
COPY sms_table_{date} FROM '{csv_dir}/{date}_sms.csv' (FORMAT CSV, HEADER TRUE);
COPY mds_table_{date} FROM '{csv_dir}/{date}_mds.csv' (FORMAT CSV, HEADER TRUE);
COPY topup_table_{date} FROM '{csv_dir}/{date}_topup.csv' (FORMAT CSV, HEADER TRUE);

DROP TABLE staging_table_{date};

CREATE TABLE staging_table_{date} AS(
	SELECT
		MSISDN,
		IMEI,
		IMSI,
		TAC,
		CELL_ID,
		DATE_TIME,
		EVENT_TYPE
	FROM call_table_{date}
	UNION ALL
	SELECT
		MSISDN,
		IMEI,
		IMSI,
		TAC,
		CELL_ID,
		DATE_TIME,
		EVENT_TYPE
	FROM location_table_{date}
	UNION ALL
	SELECT
		MSISDN,
		IMEI,
		IMSI,
		TAC,
		CELL_ID,
		DATE_TIME,
		EVENT_TYPE
	FROM sms_table_{date}
	UNION ALL
	SELECT
		MSISDN,
		IMEI,
		IMSI,
		TAC,
		CELL_ID,
		DATE_TIME,
		EVENT_TYPE
	FROM mds_table_{date}
	UNION ALL
	SELECT
		MSISDN,
		IMEI,
		IMSI,
		TAC,
		CELL_ID,
		DATE_TIME,
		EVENT_TYPE
	FROM topup_table_{date}
	ORDER BY date_time);

COMMIT;