BEGIN;

CREATE TEMPORARY TABLE call_table_{date}(
	MSISDN varchar(32),
	IMEI varchar(32),
	IMSI varchar(32),
	TAC varchar(32),
	CELL_ID varchar(5),
	DATE_TIME timestamp,
	EVENT_ID int,
	EVENT_TYPE varchar(10),
	OTHER_MSISDN varchar(32),
	DURATION real
);
CREATE TEMPORARY TABLE sms_table_{date}(
	MSISDN varchar(32),
	IMEI varchar(32),
	IMSI varchar(32),
	TAC varchar(32),
	CELL_ID varchar(5),
	DATE_TIME timestamp,
	EVENT_ID int,
	EVENT_TYPE varchar(10),
	OTHER_MSISDN varchar(32)
);
CREATE TEMPORARY TABLE location_table_{date}(
	MSISDN varchar(32),
	IMEI varchar(32),
	IMSI varchar(32),
	TAC varchar(32),
	CELL_ID varchar(5),
	DATE_TIME timestamp,
	EVENT_ID int,
	EVENT_TYPE varchar(10)
);
CREATE TEMPORARY TABLE mds_table_{date}(
	MSISDN varchar(32),
	IMEI varchar(32),
	IMSI varchar(32),
	TAC varchar(32),
	CELL_ID varchar(5),
	DATE_TIME timestamp,
	EVENT_ID int,
	EVENT_TYPE varchar(10),
	DATA_VOLUME_UP int,
	DATA_VOLUME_DOWN int
);
CREATE TEMPORARY TABLE topup_table_{date}(
	MSISDN varchar(32),
	IMEI varchar(32),
	IMSI varchar(32),
	TAC varchar(32),
	CELL_ID varchar(5),
	DATE_TIME timestamp,
	EVENT_ID int,
	EVENT_TYPE varchar(10),
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
		EVENT_ID,
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
		EVENT_ID,
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
		EVENT_ID,
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
		EVENT_ID,
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
		EVENT_ID,
		EVENT_TYPE
	FROM topup_table_{date}
	ORDER BY date_time);

COMMIT;