/*
This Source Code Form is subject to the terms of the Mozilla Public
License, v. 2.0. If a copy of the MPL was not distributed with this
file, You can obtain one at http://mozilla.org/MPL/2.0/.
*/

/*
EVENTS ---------------------------------------------------

This schema collection organizes data provided by operators
into a predictable format. We are including a number of
tables for reference (but more can be added):

  - calls:                  call data. Also called 'voice'
  - forwards:               call forwards
  - sms:                    SMS data. Also called 'text'
  - mobile_data_sessions:   mobile data sessions records
  - topups:                 records when subscribers
                            recharge their phone with
                            credit

Other data types may eventually become available.

In the `calls` and `sms` tables, the ID column should
identify parties involved in the interaction. Depending on
the case, that ID may have to be created during the ingestion
process.  It isn't enforced by the database with a
PRIMARY KEY as the process of checking for PKs would
cause a great toll to the ingestion process. IDs are not
mandatory, but a number of features created by `flowmachine`
require this field.

-----------------------------------------------------------
*/
CREATE SCHEMA IF NOT EXISTS events;

    CREATE TABLE IF NOT EXISTS events.calls(

        id TEXT,

        outgoing BOOLEAN,

        datetime TIMESTAMPTZ NOT NULL,
        duration NUMERIC,

        network TEXT,

        msisdn TEXT NOT NULL,
        msisdn_counterpart TEXT,

        location_id TEXT,

        imsi TEXT,
        imei TEXT,
        tac NUMERIC(8),

        operator_code NUMERIC,
        country_code NUMERIC

        );

    CREATE TABLE IF NOT EXISTS events.forwards(

        id TEXT,

        outgoing BOOLEAN,

        datetime TIMESTAMPTZ NOT NULL,
        network TEXT,

        msisdn TEXT NOT NULL,
        msisdn_counterpart TEXT,

        location_id TEXT,

        imsi TEXT,
        imei TEXT,
        tac NUMERIC(8),

        operator_code NUMERIC,
        country_code NUMERIC

        );

    CREATE TABLE IF NOT EXISTS events.sms(

        id TEXT,

        outgoing BOOLEAN,
        datetime TIMESTAMPTZ NOT NULL,
        network TEXT,

        msisdn TEXT NOT NULL,
        msisdn_counterpart TEXT,

        location_id TEXT,
        imsi TEXT,
        imei TEXT,
        tac NUMERIC(8),

        operator_code NUMERIC,
        country_code NUMERIC

        );

    CREATE TABLE IF NOT EXISTS events.mds(

        id TEXT,

        datetime TIMESTAMPTZ NOT NULL,
        duration NUMERIC,

        volume_total NUMERIC,
        volume_upload NUMERIC,
        volume_download NUMERIC,

        msisdn TEXT NOT NULL,

        location_id TEXT,
        imsi TEXT,
        imei TEXT,
        tac NUMERIC(8),

        operator_code NUMERIC,
        country_code NUMERIC

        );

    CREATE TABLE IF NOT EXISTS events.topups(

        id TEXT,

        datetime TIMESTAMPTZ NOT NULL,

        type TEXT,
        recharge_amount NUMERIC,
        airtime_fee NUMERIC,
        tax_and_fee NUMERIC,
        pre_event_balance NUMERIC,
        post_event_balance NUMERIC,

        msisdn TEXT NOT NULL,

        location_id TEXT,
        imsi TEXT,
        imei TEXT,
        tac NUMERIC(8),

        operator_code NUMERIC,
        country_code NUMERIC

        );
