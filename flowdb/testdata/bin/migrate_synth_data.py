# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# !/usr/bin/env python

"""
Small script for generating arbitrary volumes of CDR call data inside the flowdb
container.

Produces sites, cells, tacs, call, sms and mds data.

Optionally simulates a 'disaster' where all subscribers must leave a designated region
for a period of time.
"""
import os
import datetime
from concurrent.futures import wait
from concurrent.futures.thread import ThreadPoolExecutor
from contextlib import contextmanager
from multiprocessing import cpu_count

import sqlalchemy as sqlalchemy
from sqlalchemy.exc import ResourceClosedError

import structlog
import json

structlog.configure(
    processors=[
        structlog.stdlib.PositionalArgumentsFormatter(),
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.JSONRenderer(serializer=json.dumps),
    ]
)
logger = structlog.get_logger(__name__)


@contextmanager
def log_duration(job: str, **kwargs):
    """
    Small context handler that logs the duration of the with block.

    Parameters
    ----------
    job: str
        Description of what is being run, will be shown under the "job" key in log
    kwargs: dict
        Any kwargs will be shown in the log as "key":"value"
    """
    start_time = datetime.datetime.now()
    logger.info("Started", job=job, **kwargs)
    yield
    logger.info(
        "Finished", job=job, runtime=str(datetime.datetime.now() - start_time), **kwargs
    )


def do_exec(args):
    sql, msg, engine = args
    with log_duration(msg):
        with engine.begin() as trans:
            res = trans.execute(sql)
            try:
                logger.info(f"SQL result", job=msg, result=res.fetchall())
            except ResourceClosedError:
                pass  # Nothing to do here
            except Exception as exc:
                logger.error("Hit an issue.", exc=exc)
                raise exc


if __name__ == "__main__":
    with log_duration("Migrating synthetic data."):
        engine = sqlalchemy.create_engine(
            f"postgresql://{os.getenv('POSTGRES_USER')}@/{os.getenv('POSTGRES_DB')}",
            echo=False,
            strategy="threadlocal",
            pool_size=min(cpu_count(), int(os.getenv("MAX_CPUS", cpu_count()))),
            pool_timeout=None,
        )
        logger.info(
            "Connected.",
            num_connections=min(cpu_count(), int(os.getenv("MAX_CPUS", cpu_count()))),
        )
    with ThreadPoolExecutor(
        min(cpu_count(), int(os.getenv("MAX_CPUS", cpu_count())))
    ) as tp:
        subscribers_fut = tp.submit(
            do_exec,
            (
                """INSERT INTO interactions.subscriber (msisdn, imei, imsi, tac)
  SELECT msisdn, imei, imsi, tac FROM events.calls group by msisdn, imei, imsi, tac
  UNION
  SELECT msisdn, imei, imsi, tac FROM events.sms group by msisdn, imei, imsi, tac
  UNION
  SELECT msisdn, imei, imsi, tac FROM events.mds group by msisdn, imei, imsi, tac
  UNION
  SELECT msisdn, imei, imsi, tac FROM events.topups group by msisdn, imei, imsi, tac;""",
                "Populating subscribers.",
                engine,
            ),
        )
        locations_fut = tp.submit(
            do_exec,
            (
                """INSERT INTO interactions.locations (site_id, cell_id, position)
    SELECT sites.site_id as site_id, cells.cell_id AS cell_id, cells.geom_point as position FROM
    infrastructure.cells LEFT JOIN
    infrastructure.sites ON
        cells.site_id=sites.id AND cells.version=sites.version;""",
                "Populating locations.",
                engine,
            ),
        )
        with engine.begin():
            available_dates = [
                dt
                for dt, *_ in engine.execute(
                    "select cdr_date from etl.etl_records group by cdr_date;"
                ).fetchall()
            ]
        init_futures = [locations_fut, subscribers_fut]
        [fut.result() for fut in wait(init_futures).done]

        init_futures = [
            tp.submit(
                do_exec,
                (
                    f"""CREATE TABLE interactions.events_supertable_{dt.strftime("%Y%m%d")} PARTITION OF interactions.event_supertable FOR VALUES FROM ({dt.strftime("%Y%m%d")}) TO ({(dt + datetime.timedelta(days=1)).strftime("%Y%m%d")});""",
                    f"Adding events partition for {dt}.",
                    engine,
                ),
            )
            for dt in available_dates
        ]
        [fut.result() for fut in wait(init_futures).done]

        [fut.result() for fut in wait(init_futures).done]
        with engine.begin():
            available_dates_and_types = [
                (dt, typ)
                for dt, typ, *_ in engine.execute(
                    "SELECT DISTINCT cdr_date, cdr_type FROM etl.etl_records;"
                ).fetchall()
            ]
        with engine.begin():
            engine.execute(
                """CREATE VIEW cell_id_mapping AS (
    SELECT * FROM
    interactions.locations
        LEFT JOIN (
            SELECT cell_id, id as mno_cell_id, daterange(date_of_first_service, date_of_last_service, '[]') as valid_period FROM
            infrastructure.cells) c
        USING (cell_id)
);"""
            )
        init_futures = [
            tp.submit(
                do_exec,
                (
                    f"""CREATE TABLE interactions.{typ}_{dt.strftime("%Y%m%d")} PARTITION OF interactions.{typ} FOR VALUES FROM ({dt.strftime("%Y%m%d")}) TO ({(dt + datetime.timedelta(days=1)).strftime("%Y%m%d")});""",
                    f"Adding event subtype partition for {dt}.",
                    engine,
                ),
            )
            for dt, typ in available_dates_and_types
        ]
        [fut.result() for fut in wait(init_futures).done]
        calls_fut = tp.submit(
            do_exec,
            (
                """WITH event_data AS (SELECT
           caller_ident.subscriber_id,
           caller_loc.location_id,
           time_dim_id,
           date_dim_id,
           callee_ident.subscriber_id as called_subscriber_id,
           callee_loc.location_id as called_party_location_id,
           calling_party_msisdn,
           called_party_msisdn,
           duration,
           event_timestamp
    FROM
    (SELECT id, duration as duration, datetime as event_timestamp, location_id as caller_location_id,
            msisdn as calling_party_msisdn, tac as caller_tac FROM events.calls
        WHERE outgoing) callers
    LEFT JOIN (SELECT id, location_id as callee_location_id,
            msisdn as called_party_msisdn, tac as callee_tac FROM events.calls
        WHERE not outgoing) called
    USING (id)
    LEFT JOIN
        interactions.subscriber AS caller_ident
        ON caller_ident.msisdn=calling_party_msisdn AND caller_ident.tac=caller_tac
    LEFT JOIN
        interactions.subscriber AS callee_ident
        ON callee_ident.msisdn=called_party_msisdn AND callee_ident.tac=callee_tac
    LEFT JOIN
        cell_id_mapping AS caller_loc
        ON caller_location_id=caller_loc.mno_cell_id AND caller_loc.valid_period @> event_timestamp::date
    LEFT JOIN
        cell_id_mapping AS callee_loc
        ON callee_location_id=callee_loc.mno_cell_id AND callee_loc.valid_period @> event_timestamp::date
    LEFT JOIN
        d_date ON event_timestamp::date = date_actual
    LEFT JOIN
        d_time ON
            EXTRACT(HOUR from event_timestamp) = hour_of_day),
     call_data AS

    (INSERT INTO interactions.event_supertable (subscriber_id, location_id, time_dim_id, date_dim_id, event_timestamp, event_type_id)
        SELECT subscriber_id, location_id, time_dim_id, date_dim_id, event_timestamp, (SELECT event_type_id FROM interactions.d_event_type WHERE name='calls')
            FROM event_data
    RETURNING *)

INSERT INTO interactions.calls (event_id, date_dim_id, called_subscriber_id, called_party_location_id, calling_party_msisdn, called_party_msisdn, duration)
    SELECT event_id, date_dim_id, called_subscriber_id, called_party_location_id, calling_party_msisdn, called_party_msisdn, duration FROM call_data NATURAL JOIN event_data;""",
                "Populating calls.",
                engine,
            ),
        )
        sms_fut = tp.submit(
            do_exec,
            (
                """WITH event_data AS (SELECT caller_ident.subscriber_id,
               caller_loc.location_id,
               time_dim_id as time_dim_id,
               date_dim_id as date_dim_id,
               callee_ident.subscriber_id as called_subscriber_id,
               callee_loc.location_id as called_party_location_id,
               calling_party_msisdn,
               called_party_msisdn,
               event_timestamp
        FROM
        (SELECT id, datetime as event_timestamp, location_id as caller_location_id,
                msisdn as calling_party_msisdn, tac as caller_tac FROM events.sms
            WHERE outgoing) callers
        LEFT JOIN (SELECT id, location_id as callee_location_id,
                msisdn as called_party_msisdn, tac as callee_tac FROM events.sms
            WHERE not outgoing) called
        USING (id)
        LEFT JOIN
            interactions.subscriber AS caller_ident
            ON caller_ident.msisdn=calling_party_msisdn AND caller_ident.tac=caller_tac
        LEFT JOIN
            interactions.subscriber AS callee_ident
            ON callee_ident.msisdn=called_party_msisdn AND callee_ident.tac=callee_tac
        LEFT JOIN
            cell_id_mapping AS caller_loc
            ON caller_location_id=caller_loc.mno_cell_id AND caller_loc.valid_period @> event_timestamp::date
        LEFT JOIN
            cell_id_mapping AS callee_loc
            ON callee_location_id=callee_loc.mno_cell_id AND callee_loc.valid_period @> event_timestamp::date
        LEFT JOIN
            d_date ON event_timestamp::date = date_actual
        LEFT JOIN
            d_time ON
                EXTRACT(HOUR from event_timestamp) = hour_of_day),
         sms_data AS
        (INSERT INTO interactions.event_supertable (subscriber_id, location_id, time_dim_id, date_dim_id, event_timestamp, event_type_id)
            SELECT subscriber_id, location_id, time_dim_id, date_dim_id, event_timestamp, (SELECT event_type_id FROM interactions.d_event_type WHERE name='sms')
                FROM event_data
        RETURNING *)
    
    INSERT INTO interactions.sms (event_id, date_dim_id, called_subscriber_id, called_party_location_id, calling_party_msisdn, called_party_msisdn)
        SELECT event_id, date_dim_id, called_subscriber_id, called_party_location_id, calling_party_msisdn, called_party_msisdn FROM sms_data NATURAL JOIN event_data;""",
                "Populating sms.",
                engine,
            ),
        )
        topup_fut = tp.submit(
            do_exec,
            (
                """WITH event_data AS (SELECT caller_ident.subscriber_id,
               caller_loc.location_id,
               time_dim_id,
               date_dim_id,
               recharge_amount,
               airtime_fee,
               tax_and_fee,
               pre_event_balance,
               post_event_balance,
               calling_party_msisdn,
               caller_tac,
               event_timestamp
        FROM
        (SELECT datetime as event_timestamp, location_id as caller_location_id,
                msisdn as calling_party_msisdn, tac as caller_tac, recharge_amount,
                airtime_fee, tax_and_fee, pre_event_balance, post_event_balance
                FROM events.topups) topup
        LEFT JOIN
            interactions.subscriber AS caller_ident
            ON caller_ident.msisdn=calling_party_msisdn AND caller_ident.tac=caller_tac
        LEFT JOIN
            cell_id_mapping AS caller_loc
            ON caller_location_id=caller_loc.mno_cell_id AND caller_loc.valid_period @> event_timestamp::date
        LEFT JOIN
            d_date ON event_timestamp::date = date_actual
        LEFT JOIN
            d_time ON
                EXTRACT(HOUR from event_timestamp) = hour_of_day),
         topup_data AS
        (INSERT INTO interactions.event_supertable (subscriber_id, location_id, time_dim_id, date_dim_id, event_timestamp, event_type_id)
            SELECT subscriber_id, location_id, time_dim_id, date_dim_id, event_timestamp, (SELECT event_type_id FROM interactions.d_event_type WHERE name='topup')
                FROM event_data
        RETURNING *)
    
    INSERT INTO interactions.topup (event_id, date_dim_id, recharge_amount, airtime_fee, tax_and_fee, pre_event_balance, post_event_balance)
        SELECT event_id, date_dim_id, recharge_amount, airtime_fee, tax_and_fee, pre_event_balance, post_event_balance FROM topup_data NATURAL JOIN event_data;""",
                "Populating topups",
                engine,
            ),
        )
        mds_fut = tp.submit(
            do_exec,
            (
                """WITH event_data AS (SELECT caller_ident.subscriber_id,
                                caller_loc.location_id,
                                time_dim_id,
                                date_dim_id,
                                volume_total as data_volume_total,
                                volume_upload as data_volume_up,
                                volume_download as data_volume_down,
                                duration,
                                event_timestamp
        FROM
        (SELECT datetime as event_timestamp, location_id as caller_location_id,
                msisdn as calling_party_msisdn, tac as caller_tac, volume_total, volume_upload, volume_download,
                duration
                FROM events.mds) mds
        LEFT JOIN
            interactions.subscriber AS caller_ident
            ON caller_ident.msisdn=calling_party_msisdn AND caller_ident.tac=caller_tac
        LEFT JOIN
            cell_id_mapping AS caller_loc
            ON caller_location_id=caller_loc.mno_cell_id AND caller_loc.valid_period @> event_timestamp::date
        LEFT JOIN
            d_date ON event_timestamp::date = date_actual
        LEFT JOIN
            d_time ON
                EXTRACT(HOUR from event_timestamp) = hour_of_day),
         mds_data AS
        (INSERT INTO interactions.event_supertable (subscriber_id, location_id, time_dim_id, date_dim_id, event_timestamp, event_type_id)
            SELECT subscriber_id, location_id, time_dim_id, date_dim_id, event_timestamp, (SELECT event_type_id FROM interactions.d_event_type WHERE name='mds')
                FROM event_data
        RETURNING *)
    
    INSERT INTO interactions.mds (event_id, date_dim_id, data_volume_total, data_volume_up,
                data_volume_down,
                duration)
        SELECT event_id, date_dim_id, data_volume_total, data_volume_up,
                data_volume_down,
                duration FROM mds_data NATURAL JOIN event_data;""",
                "Populating mds.",
                engine,
            ),
        )
        geoms_fut = tp.submit(
            do_exec,
            (
                """INSERT INTO geography.geoms (short_name, long_name, geo_kind_id, spatial_resolution, geom)
        SELECT admin3pcod as short_name, admin3name as long_name, 1 as geo_kind_id, 3 as spatial_resolution, geom
            FROM geography.admin3;
    
    INSERT INTO geography.geoms (short_name, long_name, geo_kind_id, spatial_resolution, geom)
        SELECT admin2pcod as short_name, admin2name as long_name, 1 as geo_kind_id, 2 as spatial_resolution, geom
            FROM geography.admin2;
    
    INSERT INTO geography.geoms (short_name, long_name, geo_kind_id, spatial_resolution, geom)
        SELECT admin1pcod as short_name, admin1name as long_name, 1 as geo_kind_id, 1 as spatial_resolution, geom
            FROM geography.admin1;
    
    INSERT INTO geography.geoms (short_name, long_name, geo_kind_id, spatial_resolution, geom)
        SELECT admin0pcod as short_name, admin0name as long_name, 1 as geo_kind_id, 0 as spatial_resolution, geom
            FROM geography.admin0;
    
    INSERT INTO geography.geo_bridge (location_id, gid, valid_from, valid_to, linkage_method_id)
        SELECT locations.location_id, geoms.gid, '-Infinity'::date as valid_from, 'Infinity'::date as valid_to, 1 as linkage_method_id from interactions.locations LEFT JOIN geography.geoms ON ST_Intersects(position, geom);
            """,
                "Populating geoms",
                engine,
            ),
        )
        [
            fut.result()
            for fut in wait([mds_fut, topup_fut, calls_fut, sms_fut, geoms_fut]).done
        ]
        sightings_futures = []
        for dt in available_dates:
            table_name_date = dt.strftime("%Y%m%d")
            table_name = f"subscriber_sightings_{table_name_date}"
            table_d_id_start = dt.strftime("%Y%m%d")
            table_d_id_end = (dt + datetime.timedelta(days=1)).strftime("%Y%m%d")
            sightings_futures.append(
                tp.submit(
                    do_exec,
                    (
                        f"""CREATE TABLE interactions.{table_name} AS
            SELECT nextval('interactions.subscriber_sightings_sighting_id_seq'::regclass) as sighting_id, event_id, subscriber_id, location_id, time_dim_id, date_dim_id, event_timestamp as sighting_timestamp FROM interactions.event_supertable_{table_name_date}
            UNION
            SELECT nextval('interactions.subscriber_sightings_sighting_id_seq'::regclass) as sighting_id, event_id, called_subscriber_id as subscriber_id, called_party_location_id as location_id, time_dim_id, date_dim_id, event_timestamp as sighting_timestamp
                FROM interactions.event_supertable_{table_name_date} NATURAL JOIN interactions.calls_{table_name_date}
            UNION
            SELECT nextval('interactions.subscriber_sightings_sighting_id_seq'::regclass) as sighting_id, event_id, called_subscriber_id as subscriber_id, called_party_location_id as location_id, time_dim_id, date_dim_id, event_timestamp as sighting_timestamp
            FROM interactions.event_supertable_{table_name_date} NATURAL JOIN interactions.sms_{table_name_date};
            ALTER TABLE interactions.{table_name} ADD CONSTRAINT d_{table_name} CHECK (date_dim_id >= {table_d_id_start} AND date_dim_id < {table_d_id_end});
            ALTER TABLE interactions.{table_name} ADD CONSTRAINT {table_name}_date_dim_id_fkey FOREIGN KEY (date_dim_id) REFERENCES d_date(date_dim_id);
            ALTER TABLE interactions.{table_name} ADD CONSTRAINT {table_name}_event_id_date_dim_id_fkey FOREIGN KEY (event_id, date_dim_id) REFERENCES interactions.event_supertable(event_id, date_dim_id);
            ALTER TABLE interactions.{table_name} ADD CONSTRAINT {table_name}_location_id_fkey FOREIGN KEY (location_id) REFERENCES interactions.locations(location_id);
            ALTER TABLE interactions.{table_name} ADD CONSTRAINT {table_name}_time_dim_id_fkey FOREIGN KEY (time_dim_id) REFERENCES d_time(time_dim_id);
            ALTER TABLE interactions.{table_name} ADD CONSTRAINT {table_name}_pkey PRIMARY KEY(sighting_id, date_dim_id);
            ALTER TABLE interactions.{table_name} ALTER COLUMN date_dim_id SET NOT NULL;
            ALTER TABLE interactions.{table_name} ALTER COLUMN sighting_timestamp SET NOT NULL;
            ALTER TABLE interactions.{table_name} ALTER COLUMN SET DEFAULT nextval('interactions.subscriber_sightings_sighting_id_seq'::regclass);
            ALTER TABLE interactions.subscriber_sightings ATTACH PARTITION interactions.{table_name} FOR VALUES FROM ({table_d_id_start}) TO ({table_d_id_end});""",
                        f"Populate sightings for {dt}.",
                        engine,
                    ),
                )
            )

        [fut.result() for fut in wait(sightings_futures).done]
