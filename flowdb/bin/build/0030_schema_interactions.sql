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
  - locations:                  contains a new row for each time a cell
                                moves
  - event_supertable:           contains a row per call/mds/sms event. The
                                event_type field may be any of 1. calls, 2. sms, 3. mds and
                                fianlly 4. topup
  - subscriber_sightings:       contains a row per subscriber sighting event
  - calls:                      stores the additional call type data
  - sms:                        stores the additional sms type data
  - mds:                        stores the additional mobile data type data
  - topup:                      stores the additional topup type data
-----------------------------------------------------------
*/
CREATE SCHEMA IF NOT EXISTS interactions;

    CREATE TABLE IF NOT EXISTS interactions.d_event_type(
        event_type_id BIGSERIAL PRIMARY KEY,
        name VARCHAR NOT NULL
    );

    INSERT INTO interactions.d_event_type (name) VALUES ('calls'), ('sms'),  ('mds'), ('topup');

    CREATE TABLE IF NOT EXISTS interactions.subscriber(

        subscriber_id           BIGSERIAL PRIMARY KEY,
        msisdn                  TEXT,
        imei                    TEXT,
        imsi                    TEXT,
        tac                     BIGINT REFERENCES infrastructure.tacs(id),
        UNIQUE (msisdn, imei, imsi, tac)
        );

    CREATE INDEX ON interactions.subscriber (subscriber_id);
    CREATE INDEX ON interactions.subscriber (subscriber_id, msisdn);



    CREATE TABLE IF NOT EXISTS interactions.locations(
        location_id             BIGSERIAL PRIMARY KEY,
        site_id                 BIGINT REFERENCES infrastructure.sites(site_id),
        cell_id                 BIGINT REFERENCES infrastructure.cells(cell_id),
        position                geometry(POINT, 4326, 2)
        );


    CREATE INDEX IF NOT EXISTS interactions_locations_position_index
        ON interactions.locations
        USING GIST (position);

    CREATE TABLE IF NOT EXISTS interactions.event_supertable (
        event_id                BIGSERIAL NOT NULL,
        subscriber_id           BIGINT REFERENCES interactions.subscriber(subscriber_id),
        location_id             BIGINT REFERENCES interactions.locations(location_id),
        time_dim_id             BIGINT REFERENCES public.d_time(time_dim_id),
        date_dim_id             BIGINT REFERENCES public.d_date(date_dim_id),
        event_type_id           INT REFERENCES interactions.d_event_type(event_type_id),
        event_timestamp         TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (event_id, date_dim_id)
    ) PARTITION BY RANGE (date_dim_id);

    CREATE TABLE IF NOT EXISTS interactions.calls(
        event_id                    BIGINT NOT NULL,
        date_dim_id                 BIGINT NOT NULL REFERENCES public.d_date(date_dim_id),
        called_subscriber_id        BIGINT REFERENCES interactions.subscriber(subscriber_id),
        called_party_location_id    BIGINT REFERENCES interactions.locations(location_id),
        calling_party_msisdn        TEXT,
        called_party_msisdn         TEXT,
        duration                    NUMERIC,
        PRIMARY KEY (event_id, date_dim_id),
        FOREIGN KEY (event_id, date_dim_id) REFERENCES interactions.event_supertable (event_id, date_dim_id)
    ) PARTITION BY RANGE (date_dim_id);

    CREATE TABLE IF NOT EXISTS interactions.sms(
        event_id                    BIGINT NOT NULL,
        date_dim_id                 BIGINT NOT NULL REFERENCES public.d_date(date_dim_id),
        called_subscriber_id        BIGINT REFERENCES interactions.subscriber(subscriber_id),
        called_party_location_id    BIGINT REFERENCES interactions.locations(location_id),
        calling_party_msisdn        TEXT,
        called_party_msisdn         TEXT,
        PRIMARY KEY (event_id, date_dim_id),
        FOREIGN KEY (event_id, date_dim_id) REFERENCES interactions.event_supertable (event_id, date_dim_id)
    ) PARTITION BY RANGE (date_dim_id);

    CREATE TABLE IF NOT EXISTS interactions.mds(
        event_id                BIGINT NOT NULL,
        date_dim_id             BIGINT NOT NULL REFERENCES public.d_date(date_dim_id),
        data_volume_total       NUMERIC,
        data_volume_up          NUMERIC,
        data_volume_down        NUMERIC,
        duration                NUMERIC,
        PRIMARY KEY (event_id, date_dim_id),
        FOREIGN KEY (event_id, date_dim_id) REFERENCES interactions.event_supertable (event_id, date_dim_id)
    ) PARTITION BY RANGE (date_dim_id);

    CREATE TABLE IF NOT EXISTS interactions.topup(
        event_id                BIGINT NOT NULL,
        date_dim_id             BIGINT NOT NULL REFERENCES public.d_date(date_dim_id),
        type                    INTEGER,
        recharge_amount         NUMERIC,
        airtime_fee             NUMERIC,
        tax_and_fee             NUMERIC,
        pre_event_balance       NUMERIC,
        post_event_balance      NUMERIC,
        PRIMARY KEY (event_id, date_dim_id),
        FOREIGN KEY (event_id, date_dim_id) REFERENCES interactions.event_supertable (event_id, date_dim_id)
    ) PARTITION BY RANGE (date_dim_id);

    CREATE TABLE IF NOT EXISTS interactions.subscriber_sightings(
        sighting_id             BIGSERIAL,
        subscriber_id           BIGINT REFERENCES interactions.subscriber(subscriber_id),
        location_id             BIGINT REFERENCES interactions.locations(location_id),
        time_dim_id             BIGINT REFERENCES public.d_time(time_dim_id),
        date_dim_id             BIGINT REFERENCES public.d_date(date_dim_id),
        event_id                BIGINT,
        sighting_timestamp      TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (sighting_id, date_dim_id),
        FOREIGN KEY (event_id, date_dim_id) REFERENCES interactions.event_supertable (event_id, date_dim_id)

    ) PARTITION BY RANGE (date_dim_id);
    
    /* We need to an the CONSTRAINT for geography.geo_bridge here */
    ALTER TABLE geography.geo_bridge ADD CONSTRAINT locations_fkey FOREIGN KEY (location_id) REFERENCES interactions.locations(location_id);


