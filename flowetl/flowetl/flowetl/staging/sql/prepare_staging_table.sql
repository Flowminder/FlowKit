PREPARE staging_table (text) AS
    $DATE = $1;
    CREATE TABLE csv_staging_table_$1 (
        MSISDN varchar(32),
        IMEI varchar(32),
        IMSI varchar(32),
        TAC varchar(32),
        CELL_ID varchar(5),
        DATE_TIME timestamp,
        EVENT_ID int,
        EVENT_TYPE varchar(10),   -- To be replaced with enum
        OTHER_MSISDN varchar(32),
        DURATION real,
        DATA_VOLUME_UP int,
        DATA_VOLUME_DOWN int,
        RECHARGE_AMOUNT real,
        AIRTIME_FEE real,
        TAX_AND_FEE real,
        PRE_EVENT_BALANCE real,
        POST_EVENT_BALANCE real
    );
