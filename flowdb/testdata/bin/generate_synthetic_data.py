# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

#!/usr/bin/env python

"""
Small script for generating arbitrary volumes of CDR call data inside the flowdb
container. Produces csvs containing cell infrastructure data, and CDR call data,
together with SQL to ingest these CSVs, based on the SQL template found in
../synthetic_data/sql

Cell data is generated from a GeoJSON file with (simplified) admin3 boundaries for
Nepal found in ../synthetic_data/data/NPL_admbnda_adm3_Districts_simplified.geojson

Makes use of the tohu module for generation of random data.
"""

import os
from contextlib import contextmanager
from multiprocessing import cpu_count

import pandas as pd
import sqlalchemy
from sqlalchemy.exc import ResourceClosedError
from tohu import *
import argparse
import datetime
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

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

parser = argparse.ArgumentParser(description="Flowminder Synthetic CDR Generator\n")
parser.add_argument(
    "--n-subscribers", type=int, default=4000, help="Number of subscribers to generate."
)
parser.add_argument(
    "--n-cells", type=int, default=1000, help="Number of cells to generate."
)
parser.add_argument(
    "--n-calls", type=int, default=200_000, help="Number of calls to generate per day."
)
parser.add_argument(
    "--subscribers-seed",
    type=int,
    default=12345,
    help="Random seed for subscriber generation.",
)
parser.add_argument(
    "--calls-seed", type=int, default=22222, help="Random seed for calls generation."
)
parser.add_argument(
    "--cells-seed", type=int, default=99999, help="Random seed for cell generation."
)
parser.add_argument(
    "--n-days", type=int, default=7, help="Number of days of data to generate."
)
parser.add_argument(
    "--output-root-dir",
    type=str,
    default="",
    help="Root directory under which output .csv and .sql files are stored (in appropriate subfolders).",
)


class CellGenerator(CustomGenerator):
    cell_id = DigitString(length=8)
    site_id = Sequential(prefix="", digits=1)
    version = SelectOne([0, 1, 2])
    date_of_first_service = Constant("2016-01-01")
    date_of_last_service = Constant("")
    longitude, latitude = GeoJSONGeolocation(
        "/opt/synthetic_data/NPL_admbnda_adm3_Districts_simplified.geojson"
    ).split()


class SubscriberGenerator(CustomGenerator):
    msisdn = HashDigest(length=40)
    imei = HashDigest(length=40)
    imsi = HashDigest(length=40)
    tac = DigitString(length=5)
    int_prefix = DigitString(length=5)


class CallEventGenerator(CustomGenerator):
    def __init__(self, *, subscribers, cells, date):
        self.a_party = SelectOne(subscribers)
        self.b_party = SelectOne(subscribers)
        self.cell_from = SelectOne(cells)
        self.cell_to = SelectOne(cells)

        self.start_time = Timestamp(date=date)
        self.duration = Integer(0, 2600)
        self.call_id = HashDigest(length=8)

        super().__init__(subscribers, cells, date)


def convert_to_two_line_format(df):
    """
    Given a dataframe with calls in one-line format (i.e., with a-party
    and b-party noth in the same row), return a dataframe with the same
    calls in two-line format (with a-party and b-party information split
    across two separate lines for each call).

    Note that this is not the most efficient implementation but it is
    sufficient for our purposes.
    """
    df_outgoing = df[
        [
            "start_time",
            "msisdn_to",
            "id",
            "msisdn_from",
            "cell_id_from",
            "duration",
            "tac_from",
        ]
    ].copy()
    df_outgoing = df_outgoing.rename(
        columns={
            "msisdn_to": "msisdn_counterpart",
            "msisdn_from": "msisdn",
            "cell_id_from": "location_id",
            "tac_from": "tac",
        }
    )
    df_outgoing.loc[:, "outgoing"] = True

    df_incoming = df[
        [
            "start_time",
            "msisdn_from",
            "id",
            "msisdn_to",
            "cell_id_to",
            "duration",
            "tac_to",
        ]
    ].copy()
    df_incoming = df_incoming.rename(
        columns={
            "msisdn_from": "msisdn_counterpart",
            "msisdn_to": "msisdn",
            "cell_id_to": "location_id",
            "tac_to": "tac",
        }
    )
    df_incoming.loc[:, "outgoing"] = False

    columns = [
        "start_time",
        "msisdn_counterpart",
        "id",
        "msisdn",
        "location_id",
        "outgoing",
        "duration",
        "tac",
    ]
    df_full = pd.concat([df_outgoing, df_incoming])
    df_full = df_full[columns]  # bring columns into expected order
    return df_full


def write_day_csv(subscribers, cells, date, num_calls, call_seed, output_root_dir):
    """
    Generate a day of data and write it to a csv, return the sql to ingest it.

    Parameters
    ----------
    subscribers: list
        List of objects with msisdn, imei, imsi, tac and int_prefix attributes
    cells: list
        List of objects with cell_id, site_id, version, date_of_first_service, date_of_last_service,
        lon, and lat attributes.
    date: datetime.date
        Date calls should occur on
    num_calls: int
        Number of calls to generate
    call_seed: int
        Random seed to use for generating calls
    outut_root_dir: str
        Root directory under which output CSV files are stored (in appropriate subfolders).

    Returns
    -------
    str
        SQL command string which will ingest the written CSV file.
    """
    ceg = CallEventGenerator(
        subscribers=subscribers, cells=cells, date=date.strftime("%Y-%m-%d")
    )
    fields = {
        "id": "call_id",
        "start_time": "start_time",
        "duration": "duration",
        "msisdn_from": "a_party.msisdn",
        "msisdn_to": "b_party.msisdn",
        "tac_from": "a_party.tac",
        "tac_to": "b_party.tac",
        "cell_id_from": "cell_from.cell_id",
        "cell_id_to": "cell_to.cell_id",
    }
    os.makedirs(f"{output_root_dir}/data/records/calls", exist_ok=True)
    fpath = f"{output_root_dir}/data/records/calls/calls_{date.strftime('%Y%m%d')}.csv"
    with log_duration("Generating calls."):
        calls_df = ceg.generate(num_calls, seed=call_seed).to_df(fields=fields)
    with log_duration("Converting calls to two-line."):
        calls_df_twoline = convert_to_two_line_format(calls_df)
    with log_duration("Writing calls to csv."):
        calls_df_twoline.to_csv(fpath, index=False)

    ingest_sql = """
            CREATE TABLE IF NOT EXISTS events.calls_{table} (
                    CHECK ( datetime >= '{table}'::TIMESTAMPTZ
                    AND datetime < '{end_date}'::TIMESTAMPTZ)
                ) INHERITS (events.calls);
                ALTER TABLE events.calls_{table} NO INHERIT events.calls;

                COPY events.calls_{table}( datetime,msisdn_counterpart,id,msisdn,location_id,outgoing,duration,tac )
                    FROM '{output_root_dir}/data/records/calls/calls_{table}.csv'
                        WITH DELIMITER ','
                        CSV HEADER;
                CREATE INDEX ON events.calls_{table} (msisdn);

            CREATE INDEX ON events.calls_{table} (msisdn_counterpart);
            CREATE INDEX ON events.calls_{table} (tac);
            CREATE INDEX ON events.calls_{table} (location_id);
            CREATE INDEX ON events.calls_{table} (datetime);
            CLUSTER events.calls_{table} USING calls_{table}_msisdn_idx;
            ANALYZE events.calls_{table};
            ALTER TABLE events.calls_{table} INHERIT events.calls;""".format(
        output_root_dir=output_root_dir,
        table=date.strftime("%Y%m%d"),
        end_date=(date + datetime.timedelta(days=1)).strftime("%Y%m%d"),
    )
    return f"Inserting {num_calls} calls for {date}.", ingest_sql


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


if __name__ == "__main__":
    args = parser.parse_args()
    with log_duration("Generating synthetic data", **vars(args)):
        num_subscribers = args.n_subscribers
        num_cells = args.n_cells
        num_calls = args.n_calls

        subscriber_seed = args.subscribers_seed
        cell_seed = args.cells_seed
        call_seed = args.calls_seed

        output_root_dir = args.output_root_dir
        os.makedirs(
            os.path.join(output_root_dir, "data", "infrastructure"), exist_ok=True
        )

        with log_duration(f"Generating {num_subscribers} subscribers."):
            sg = SubscriberGenerator()
            subscribers = list(sg.generate(num_subscribers, seed=subscriber_seed))

        with log_duration(f"Generating {num_cells} cells."):
            cg = CellGenerator()
            cells = cg.generate(num_cells, seed=cell_seed)
            cells.to_csv(
                os.path.join(output_root_dir, "data", "infrastructure", "cells.csv"),
                fields=[
                    "cell_id",
                    "site_id",
                    "version",
                    "longitude",
                    "latitude",
                    "date_of_first_service",
                    "date_of_last_service",
                ],
            )

        cells_ingest_sql = f"""
        DELETE FROM infrastructure.cells;
    
        CREATE TEMP TABLE temp_cells (
            id TEXT,
            site_id TEXT,
            version NUMERIC,
            longitude NUMERIC,
            latitude NUMERIC,
            date_of_first_service TEXT,
            date_of_last_service TEXT
        );
        
        COPY temp_cells (
                id,
                site_id,
                version,
                longitude,
                latitude,
                date_of_first_service,
                date_of_last_service
            )
        FROM
            '{os.path.join(output_root_dir, "data", "infrastructure", "cells.csv")}'
        WITH
            ( DELIMITER ',',
            HEADER true,
            FORMAT csv );
        
        INSERT INTO infrastructure.cells (
            id,
            site_id,
            version,
            date_of_first_service,
            date_of_last_service,
            geom_point
            )
                SELECT
                    id,
                    site_id,
                    version,
                    date_of_first_service::date,
                    date_of_last_service::date,
                    ST_SetSRID(ST_MakePoint(longitude, latitude), 4326) AS geom_point
                FROM temp_cells;
        INSERT INTO infrastructure.sites(id, version, date_of_first_service, date_of_last_service, geom_point) 
            SELECT site_id, version, date_of_first_service, date_of_last_service, geom_point FROM infrastructure.cells;
        """

        with log_duration(f"Generating {args.n_days} days of calls."):
            dates = pd.date_range(start="2016-01-01", periods=args.n_days)

            def write_f(seed_inc, date):
                return write_day_csv(
                    subscribers,
                    cells,
                    date,
                    num_calls,
                    call_seed + seed_inc,
                    output_root_dir,
                )

            with ProcessPoolExecutor() as pool:
                tables = list(pool.map(write_f, *zip(*enumerate(dates))))
        tables.append((f"Inserting {num_cells} cells.", cells_ingest_sql))

        # Run ingest on multiple threads
        engine = sqlalchemy.create_engine(
            f"postgresql://{os.getenv('POSTGRES_USER')}@/{os.getenv('POSTGRES_DB')}",
            echo=False,
            pool_size=cpu_count(),
            pool_timeout=None,
        )

        def do_exec(args):
            msg, sql = args
            with log_duration(msg):
                started = datetime.datetime.now()
                with engine.begin() as trans:
                    res = trans.execute(sql)
                    try:
                        logger.info(f"Ran", job=msg, result=res.fetchall())
                    except ResourceClosedError:
                        pass  # Nothing to do here

        do_exec(("Ensuring events.calls is empty.", "DELETE FROM events.calls;"))
        with ThreadPoolExecutor(cpu_count()) as tp:
            list(tp.map(do_exec, tables))
        do_exec(("Analyzing events.calls.", "ANALYZE events.calls;"))
        do_exec(
            (
                "Marking tables as available.",
                """
            INSERT INTO available_tables (table_name, has_locations, has_subscribers, has_counterparts) VALUES ('calls', true, true, true)
            ON conflict (table_name)
            DO UPDATE SET has_locations=EXCLUDED.has_locations, has_subscribers=EXCLUDED.has_subscribers, has_counterparts=EXCLUDED.has_counterparts;""",
            )
        )
        for date in pd.date_range(start="2016-01-01", periods=args.n_days):
            do_exec(
                (
                    "Marking day as ingested.",
                    f"""
    INSERT INTO etl.etl_records (cdr_type, cdr_date, state, timestamp) VALUES
    ('calls', '{date}'::DATE, 'ingested', NOW());""",
                )
            )
