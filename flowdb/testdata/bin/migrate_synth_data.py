from pathlib import Path

import os
import argparse
import datetime
from concurrent.futures.thread import ThreadPoolExecutor
from contextlib import contextmanager
from multiprocessing import cpu_count

import sqlalchemy as sqlalchemy
from sqlalchemy.exc import ResourceClosedError
import json

try:
    import structlog

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
except ImportError:
    import logging

    logger = logging.getLogger(__name__)
    logger.setLevel("DEBUG")


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
    try:
        logger.info("Started", job=job, **kwargs)
    except:
        logger.info(f"Started {job}: {kwargs}")
    yield
    try:
        logger.info(
            "Finished",
            job=job,
            runtime=str(datetime.datetime.now() - start_time),
            **kwargs,
        )
    except:
        logger.info(
            f"Finished {job}. runtime={str(datetime.datetime.now() - start_time)}, {kwargs}"
        )


parser = argparse.ArgumentParser(description="Flowminder Synthetic CDR Migrator\n")
parser.add_argument(
    "--n-days",
    type=int,
    default=os.getenv("N_DAYS", 7),
    help="Number of days of data to migrate.",
)

if __name__ == "__main__":
    args = parser.parse_args()
    with log_duration("Migrating synthetic data..", **vars(args)):
        num_days = args.n_days
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

        def do_exec(args):
            msg, sql = args
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

        start_time = datetime.datetime.now()

        for date in (
            datetime.date(2016, 1, 1) + datetime.timedelta(days=i)
            for i in range(num_days)
        ):
            with log_duration("Migrating day.", day=date):
                partition_period = f"FROM ({date.strftime('%Y%M%d')}) TO ({(date + datetime.timedelta(days=1)).strftime('%Y%M%d')})"
                with log_duration("Creating partitions.", day=date):
                    with engine.begin() as trans:
                        trans.execute(
                            f"CREATE TABLE interactions.events_supertable_{date.strftime('%Y%M%d')} PARTITION OF interactions.event_supertable FOR VALUES {partition_period};"
                        )
                        trans.execute(
                            f"CREATE TABLE interactions.subscriber_sightings_{date.strftime('%Y%M%d')} PARTITION OF interactions.subscriber_sightings FOR VALUES {partition_period};"
                        )
                        for event_type in ("calls", "sms", "mds", "topups"):
                            trans.execute(
                                f"CREATE TABLE interactions.calls_{date.strftime('%Y%M%d')} PARTITION OF interactions.{event_type} FOR VALUES {partition_period};"
                            )
        with log_duration("Migrate subscribers."):
            with engine.begin() as trans:
                trans.execute(
                    """
                INSERT INTO interactions.subscriber (msisdn, imei, imsi, tac)
                  SELECT msisdn, imei, imsi, tac FROM events.calls group by msisdn, imei, imsi, tac
                  UNION
                  SELECT msisdn, imei, imsi, tac FROM events.sms group by msisdn, imei, imsi, tac
                  UNION
                  SELECT msisdn, imei, imsi, tac FROM events.mds group by msisdn, imei, imsi, tac
                  UNION
                  SELECT msisdn, imei, imsi, tac FROM events.topups group by msisdn, imei, imsi, tac;
                """
                )
                trans.execute(
                    """
                INSERT INTO interactions.locations (site_id, cell_id, position)
                    SELECT sites.site_id as site_id, cells.cell_id AS cell_id, cells.geom_point as position FROM
                    infrastructure.cells LEFT JOIN
                    infrastructure.sites ON
                        cells.site_id=sites.id AND cells.version=sites.version;
                """
                )
                trans.execute(
                    """
                CREATE VIEW cell_id_mapping AS (
                    SELECT * FROM
                    interactions.locations
                        LEFT JOIN (
                            SELECT cell_id, id as mno_cell_id, daterange(date_of_first_service, date_of_last_service, '[]') as valid_period FROM
                            infrastructure.cells) c
                        USING (cell_id)
                );
                """
                )

        with log_duration("Migrate geography"):
            with engine.begin() as trans:
                trans.execute(
                    """
                INSERT INTO geography.geoms (short_name, long_name, geo_kind_id, spatial_resolution, geom)
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
                
                INSERT INTO geography.geoms (short_name, long_name, geo_kind_id, spatial_resolution, geom)
                    SELECT district_c as short_name, district_n as long_name, 1 as geo_kind_id, 2 as spatial_resolution, geom
                        FROM public.gambia_admin2;
                
                /* Populate the geobridge */
                
                INSERT INTO geography.geo_bridge (location_id, gid, valid_from, valid_to, linkage_method_id)
                    SELECT locations.location_id, geoms.gid, '-Infinity'::date as valid_from, 'Infinity'::date as valid_to, 1 as linkage_method_id from interactions.locations LEFT JOIN geography.geoms ON ST_Intersects(position, geom);
                """
                )

        events = [
            (
                """
            WITH event_data AS (SELECT
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
            SELECT event_id, date_dim_id, called_subscriber_id, called_party_location_id, calling_party_msisdn, called_party_msisdn, duration FROM call_data NATURAL JOIN event_data;
            """,
                "Call events",
            ),
            (
                """
            WITH event_data AS (SELECT caller_ident.subscriber_id,
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
            SELECT event_id, date_dim_id, called_subscriber_id, called_party_location_id, calling_party_msisdn, called_party_msisdn FROM sms_data NATURAL JOIN event_data;
            """,
                "SMS events",
            ),
            (
                """
            WITH event_data AS (SELECT caller_ident.subscriber_id,
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
                    duration FROM mds_data NATURAL JOIN event_data;
            """,
                "MDS events",
            ),
            (
                """
            WITH event_data AS (SELECT caller_ident.subscriber_id,
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
            SELECT event_id, date_dim_id, recharge_amount, airtime_fee, tax_and_fee, pre_event_balance, post_event_balance FROM topup_data NATURAL JOIN event_data;
            """,
                "Topup events",
            ),
        ]
        with log_duration("Migrate events."):
            with ThreadPoolExecutor(
                min(cpu_count(), int(os.getenv("MAX_CPUS", cpu_count())))
            ) as tp:
                list(tp.map(do_exec, events))
        with log_duration("Migrate sightings."):
            with engine.begin() as trans:
                trans.execute(
                    """
                INSERT INTO interactions.subscriber_sightings (event_id, subscriber_id, location_id, time_dim_id, date_dim_id, sighting_timestamp)
                    SELECT event_id, subscriber_id, location_id, time_dim_id, date_dim_id, event_timestamp FROM interactions.event_supertable;
                
                INSERT INTO interactions.subscriber_sightings (event_id, subscriber_id, location_id, time_dim_id, date_dim_id, sighting_timestamp)
                    SELECT event_id, called_subscriber_id as subscriber_id, called_party_location_id as location_id, time_dim_id, date_dim_id, event_timestamp
                        FROM interactions.event_supertable NATURAL JOIN interactions.calls;
                
                INSERT INTO interactions.subscriber_sightings (event_id, subscriber_id, location_id, time_dim_id, date_dim_id, sighting_timestamp)
                    SELECT event_id, called_subscriber_id as subscriber_id, called_party_location_id as location_id, time_dim_id, date_dim_id, event_timestamp
                        FROM interactions.event_supertable NATURAL JOIN interactions.sms;
                """
                )
