/*
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.
*/

/*
Interaction -------------------------------------------------------

This schema contains tables related to interactions between 
subscribers.

  - subscriber:                 subscriber encountered in the event 
                                data.
  - date_dim:                   stores relevant information about a 
                                specific date.
  - time_dimension:             stores relevant information about a 
                                specific time.
  - locations:                  contains a new row for each time a cell
                                moves
  - receiving_parties:
  - event_supertable:           contains a row per call/mds/sms event. The
                                event_type field can be an integer for 
                                each type 1. calls, 2. sms, 3. mds and
                                fianlly 4. topup
  - subscriber_sightings:       contains a row per subscriber sighting event
  - calls:                      stores the additional call type data
  - sms:                        stores the additional sms type data
  - mds:                        stores the additional mobile data type data
  - topup:                      stores the additional topup type data
-----------------------------------------------------------
*/
CREATE SCHEMA IF NOT EXISTS interactions;

    CREATE TABLE IF NOT EXISTS interactions.subscriber(

        id                      BIGSERIAL PRIMARY KEY,
        msisdn                  TEXT,
        imei                    TEXT,
        imsi                    TEXT,
        tac                     BIGINT REFERENCES infrastructure.tacs(id)

        );

    CREATE INDEX ON interactions.subscriber (id);
    CREATE INDEX ON interactions.subscriber (id, msisdn);

    CREATE TABLE d_date
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

    ALTER TABLE public.d_date ADD CONSTRAINT d_date_date_dim_id_pk PRIMARY KEY (date_dim_id);

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
    FROM (SELECT '1979-01-01'::DATE+ SEQUENCE.DAY AS datum
          FROM GENERATE_SERIES (0,29219) AS SEQUENCE (DAY)
          GROUP BY SEQUENCE.DAY) DQ
    ORDER BY 1;

    CREATE INDEX d_date_date_actual_idx
        ON d_date(date_actual);

    CREATE TABLE IF NOT EXISTS d_time(
        time_dim_id             INT PRIMARY KEY,
        time_of_day             VARCHAR(5),
        hour_of_day             INT,
        minute_of_day           INT,
        meridean_indicator      VARCHAR(2),
        minute_of_hour          INT
        );

    INSERT INTO d_time
    SELECT
        -- Primary key
        to_char(MINUTE,'hh24mi')::INT AS time_dim_id,
        -- Time of day
        to_char(MINUTE, 'hh24:mi') AS time_of_day,
        -- Hour of the day (0 - 23)
        EXTRACT(HOUR FROM MINUTE) AS hour_of_day,
        -- Minute of the day (0 - 1439)
        EXTRACT(HOUR FROM MINUTE)*60 + EXTRACT(MINUTE FROM MINUTE) AS minute_of_day,
        -- AM/PM
        to_char(MINUTE, 'am') AS meridean_indicator,
        -- Minute of the hour
        EXTRACT(MINUTE FROM MINUTE) AS minute_of_hour

    FROM (SELECT '0:00'::TIME + (SEQUENCE.MINUTE || ' minutes')::INTERVAL AS MINUTE
        FROM generate_series(0,1439) AS SEQUENCE(MINUTE)
        GROUP BY SEQUENCE.MINUTE
         ) DQ
    ORDER BY 1;

    CREATE TABLE IF NOT EXISTS interactions.locations(

        cell_id                 BIGSERIAL PRIMARY KEY,
        site_id                 TEXT,
        mno_cell_code           TEXT

        );

    SELECT AddGeometryColumn('interactions', 'locations', 'position', 4326, 'POINT', 2);

    CREATE INDEX IF NOT EXISTS interactions_locations_position_index
        ON interactions.locations
        USING GIST (position);

    CREATE TABLE IF NOT EXISTS interactions.receiving_parties(

        parties_key             BIGSERIAL PRIMARY KEY,
        cell_id                 BIGINT REFERENCES interactions.locations(cell_id),
        party_msisdn            TEXT

        );
    
    CREATE TABLE IF NOT EXISTS interactions.event_supertable (

        event_id                BIGSERIAL,
        subscriber_id           BIGINT REFERENCES interactions.subscriber(id),
        cell_id                 BIGINT REFERENCES interactions.locations(cell_id),
        time_sk                 BIGINT REFERENCES public.d_time(time_dim_id),
        date_sk                 BIGINT REFERENCES public.d_date(date_dim_id),
        event_type              INTEGER,
        timestamp               TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (event_id, date_sk)

    ) PARTITION BY LIST (date_sk);

    CREATE TABLE IF NOT EXISTS interactions.subscriber_sightings(

        sighting_id             BIGSERIAL,
        subscriber_id           BIGINT REFERENCES interactions.subscriber(id),
        cell_id                 BIGINT REFERENCES interactions.locations(cell_id),
        time_sk                 BIGINT REFERENCES public.d_time(time_dim_id),
        date_sk                 BIGINT REFERENCES public.d_date(date_dim_id),
        event_super_table_id    BIGINT, /* REFERENCES interactions.event_supertable(event_id), - this can be added in PG12 */ 
        timestamp               TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (sighting_id, date_sk)

    ) PARTITION BY LIST (date_sk);
    
    /* We need to an the CONSTRAINT for geography.geo_bridge here */
    ALTER TABLE geography.geo_bridge ADD CONSTRAINT locations_fkey FOREIGN KEY (cell_id) REFERENCES interactions.locations(cell_id);

    CREATE TABLE IF NOT EXISTS interactions.calls(

        super_table_id          BIGSERIAL,
        subscriber_id           BIGINT REFERENCES interactions.subscriber(id),
        called_subscriber_id    BIGINT REFERENCES interactions.subscriber(id),
        calling_party_cell_id   BIGINT REFERENCES interactions.locations(cell_id),
        called_party_cell_id    BIGINT REFERENCES interactions.locations(cell_id),
        time_sk                 BIGINT REFERENCES public.d_time(time_dim_id),
        date_sk                 BIGINT REFERENCES public.d_date(date_dim_id),
        calling_party_msisdn    TEXT,
        called_party_msisdn     TEXT,
        call_duration           NUMERIC,
        timestamp               TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (super_table_id, date_sk)

    ) PARTITION BY LIST (date_sk);
    
    CREATE TABLE IF NOT EXISTS interactions.sms(

        super_table_id                BIGSERIAL,
        subscriber_id                 BIGINT REFERENCES interactions.subscriber(id),
        calling_party_cell_id         BIGINT REFERENCES interactions.locations(cell_id),
        parties_key                   BIGINT REFERENCES interactions.receiving_parties(parties_key),
        time_sk                       BIGINT REFERENCES public.d_time(time_dim_id),
        date_sk                       BIGINT REFERENCES public.d_date(date_dim_id),
        calling_party_msisdn          TEXT,
        receiving_parties_msisdns     TEXT,
        timestamp                     TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (super_table_id, date_sk)

    ) PARTITION BY LIST (date_sk);
    
    CREATE TABLE IF NOT EXISTS interactions.mds(

        super_table_id          BIGSERIAL,
        subscriber_id           BIGINT REFERENCES interactions.subscriber(id),
        cell_id                 BIGINT REFERENCES interactions.locations(cell_id),
        time_sk                 BIGINT REFERENCES public.d_time(time_dim_id),
        date_sk                 BIGINT REFERENCES public.d_date(date_dim_id),
        data_volume_total       NUMERIC,
        data_volume_up          NUMERIC,
        data_volume_down        NUMERIC,
        duration                NUMERIC,
        timestamp               TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (super_table_id, date_sk)

    ) PARTITION BY LIST (date_sk);

    CREATE TABLE IF NOT EXISTS interactions.topup(

        super_table_id          BIGSERIAL,
        subscriber_id           BIGINT REFERENCES interactions.subscriber(id),
        cell_id                 BIGINT REFERENCES interactions.locations(cell_id),
        time_sk                 BIGINT REFERENCES public.d_time(time_dim_id),
        date_sk                 BIGINT REFERENCES public.d_date(date_dim_id),
        type                    INTEGER,
        recharge_amount         NUMERIC,
        airtime_fee             NUMERIC,
        tax_and_fee             NUMERIC,
        pre_event_balance       NUMERIC,
        post_event_balance      NUMERIC,
        timestamp               TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (super_table_id, date_sk)

    ) PARTITION BY LIST (date_sk);
