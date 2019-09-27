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
  - subscriber_sightings_fact:  contains a row per subscriber - with one
                                event expanding to multiple sightings.
  - locations:                  contains a new row for each time a 
                                subscriber moves.
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

    CREATE TABLE IF NOT EXISTS interactions.date_dim(

        date_sk                 SERIAL PRIMARY KEY,
        date                    TIMESTAMPTZ,
        day_of_week             TEXT,
        day_of_month            TEXT,
        year                    TEXT

        );

    CREATE TABLE IF NOT EXISTS interactions.time_dimension(

        time_sk                 SERIAL PRIMARY KEY,
        hour                    NUMERIC

        );

    CREATE TABLE IF NOT EXISTS interactions.subscriber_sightings_fact(

        sighting_id             BIGSERIAL,
        subscriber_id           BIGINT REFERENCES interactions.subscriber(id),
        cell_id                 BIGINT REFERENCES interactions.locations(cell_id),
        date_sk                 BIGINT REFERENCES interactions.date_dim(date_sk),
        time_sk                 BIGINT REFERENCES interactions.time_dimension(time_sk),
        event_super_table_id    TEXT,
        event_type              INTEGER,
        timestamp               TIMESTAMPTZ NOT NULL,
        PRIMARY KEY (sighting_id, date_sk)

    ) PARTITION BY LIST (date_sk);

    CREATE TABLE IF NOT EXISTS interactions.locations(

        cell_id                 BIGSERIAL PRIMARY KEY,
        site_id                 TEXT,
        mno_cell_code           TEXT

        );
    
    SELECT AddGeometryColumn('interactions', 'locations', 'position', 4326, 'POINT', 2);

    CREATE INDEX IF NOT EXISTS interactions_locations_position_index
        ON interactions.locations
        USING GIST (position);
