/*
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.
*/

/*
PUBLIC ---------------------------------------------

Here we define the public schema. This schema

contains:

- files
 this table is designed for the purposes of keeping track
    of the ingestion of text files.

    Available columns:

      - files:  where information about the ingestion of
                files is kept.

- d_date:                   General purpose date dimension table.
- d_time:                   General purpose time dimension table.

----------------------------------------------------
*/
CREATE TABLE IF NOT EXISTS public.files(
    
    id TEXT PRIMARY KEY,
    full_path TEXT,
    directory_path TEXT,
    specification_path TEXT,
    target_table TEXT,
    created TIMESTAMPTZ,
    processed BOOLEAN,
    datetime TIMESTAMPTZ,
    size NUMERIC,
    status TEXT,
    record_number NUMERIC,
    log TEXT

    );

    CREATE TABLE IF NOT EXISTS d_date
    (
      date_dim_id              INT PRIMARY KEY,
      date_actual              DATE NOT NULL,
      epoch                    BIGINT NOT NULL,
      day_suffix               VARCHAR(4) NOT NULL,
      day_name                 VARCHAR(9) NOT NULL,
      day_of_week              INT NOT NULL,
      day_of_month             INT NOT NULL,
      day_of_quarter           INT NOT NULL,
      day_of_year              INT NOT NULL,
      week_of_month            INT NOT NULL,
      week_of_year             INT NOT NULL,
      week_of_year_iso         CHAR(10) NOT NULL,
      month_actual             INT NOT NULL,
      month_name               VARCHAR(9) NOT NULL,
      month_name_abbreviated   CHAR(3) NOT NULL,
      quarter_actual           INT NOT NULL,
      quarter_name             VARCHAR(9) NOT NULL,
      year_actual              INT NOT NULL,
      first_day_of_week        DATE NOT NULL,
      last_day_of_week         DATE NOT NULL,
      first_day_of_month       DATE NOT NULL,
      last_day_of_month        DATE NOT NULL,
      first_day_of_quarter     DATE NOT NULL,
      last_day_of_quarter      DATE NOT NULL,
      first_day_of_year        DATE NOT NULL,
      last_day_of_year         DATE NOT NULL,
      mmyyyy                   CHAR(6) NOT NULL,
      mmddyyyy                 CHAR(10) NOT NULL,
      is_std_weekend           BOOLEAN NOT NULL
    );


    INSERT INTO d_date
    SELECT TO_CHAR(datum,'yyyymmdd')::INT AS date_dim_id,
           datum AS date_actual,
           EXTRACT(epoch FROM datum) AS epoch,
           TO_CHAR(datum,'fmDDth') AS day_suffix,
           TO_CHAR(datum,'Day') AS day_name,
           EXTRACT(isodow FROM datum) AS day_of_week,
           EXTRACT(DAY FROM datum) AS day_of_month,
           datum - DATE_TRUNC('quarter',datum)::DATE +1 AS day_of_quarter,
           EXTRACT(doy FROM datum) AS day_of_year,
           TO_CHAR(datum,'W')::INT AS week_of_month,
           EXTRACT(week FROM datum) AS week_of_year,
           TO_CHAR(datum,'YYYY"-W"IW-') || EXTRACT(isodow FROM datum) AS week_of_year_iso,
           EXTRACT(MONTH FROM datum) AS month_actual,
           TO_CHAR(datum,'Month') AS month_name,
           TO_CHAR(datum,'Mon') AS month_name_abbreviated,
           EXTRACT(quarter FROM datum) AS quarter_actual,
           CASE
             WHEN EXTRACT(quarter FROM datum) = 1 THEN 'First'
             WHEN EXTRACT(quarter FROM datum) = 2 THEN 'Second'
             WHEN EXTRACT(quarter FROM datum) = 3 THEN 'Third'
             WHEN EXTRACT(quarter FROM datum) = 4 THEN 'Fourth'
           END AS quarter_name,
           EXTRACT(isoyear FROM datum) AS year_actual,
           datum +(1 -EXTRACT(isodow FROM datum))::INT AS first_day_of_week,
           datum +(7 -EXTRACT(isodow FROM datum))::INT AS last_day_of_week,
           datum +(1 -EXTRACT(DAY FROM datum))::INT AS first_day_of_month,
           (DATE_TRUNC('MONTH',datum) +INTERVAL '1 MONTH - 1 day')::DATE AS last_day_of_month,
           DATE_TRUNC('quarter',datum)::DATE AS first_day_of_quarter,
           (DATE_TRUNC('quarter',datum) +INTERVAL '3 MONTH - 1 day')::DATE AS last_day_of_quarter,
           TO_DATE(EXTRACT(isoyear FROM datum) || '-01-01','YYYY-MM-DD') AS first_day_of_year,
           TO_DATE(EXTRACT(isoyear FROM datum) || '-12-31','YYYY-MM-DD') AS last_day_of_year,
           TO_CHAR(datum,'mmyyyy') AS mmyyyy,
           TO_CHAR(datum,'mmddyyyy') AS mmddyyyy,
           CASE
             WHEN EXTRACT(isodow FROM datum) IN (6,7) THEN TRUE
             ELSE FALSE
           END AS is_std_weekend
    FROM (SELECT date '1979-01-01' + DAY AS datum
          FROM generate_series(0,29219) AS SEQUENCE (DAY)) DQ
    ORDER BY 1;

    CREATE INDEX d_date_date_actual_idx
        ON d_date(date_actual);

    CREATE TABLE IF NOT EXISTS d_time(
        time_dim_id             INT PRIMARY KEY,
        time_of_day             TIME,
        hour_of_day             INT,
        meridian_indicator      CHAR(2)
        );

    INSERT INTO d_time
    SELECT
        -- Primary key
        to_char(hour,'hh24')::INT AS time_dim_id,
        -- Time of day
        hour AS time_of_day,
        -- Hour of the day (0 - 23)
        EXTRACT(HOUR FROM hour) AS hour_of_day,
        -- AM/PM
        to_char(hour, 'am') AS meridian_indicator

    FROM (SELECT time '0:00' + SEQUENCE.HOUR * interval '1 hour' AS hour
        FROM generate_series(0,23) AS SEQUENCE(HOUR)
         ) DQ
    ORDER BY 1;
