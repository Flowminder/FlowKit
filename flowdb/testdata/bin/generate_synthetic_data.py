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
from collections import OrderedDict
import pandas as pd
import textwrap
import tohu
from tohu import *
import argparse
import datetime
from concurrent.futures import ProcessPoolExecutor

# here = os.path.dirname(os.path.abspath(__file__))

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
    calls_df = ceg.generate(num_calls, seed=call_seed).to_df(fields=fields)
    calls_df_twoline = convert_to_two_line_format(calls_df)
    calls_df_twoline.to_csv(fpath, index=False)

    ingest_sql = """
        BEGIN;
            CREATE TABLE IF NOT EXISTS events.calls_{table} (
                    CHECK ( datetime >= '{table}'::TIMESTAMPTZ
                    AND datetime < '{end_date}'::TIMESTAMPTZ)
                ) INHERITS (events.calls);

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
        COMMIT;""".format(
        output_root_dir=output_root_dir,
        table=date.strftime("%Y%m%d"),
        end_date=(date + datetime.timedelta(days=1)).strftime("%Y%m%d"),
    )
    return ingest_sql


def write_ingest_sql(tables, output_root_dir):
    """
    Write to `{output_root_dir}/sql/syntheticdata/ingest_calls.sql` an ingestion
    script that wraps the list of table creation statements provided in tables.

    Parameters
    ----------
    tables: list of str
        List of SQL strings, each of which is a table creation statement.
    """
    ingest_head = """
        BEGIN;
        DELETE FROM events.calls;"""
    ingest_tail = """
        ANALYZE events.calls;
        INSERT INTO available_tables (table_name, has_locations, has_subscribers, has_counterparts) VALUES ('calls', true, true, true)
        ON conflict (table_name)
        DO UPDATE SET has_locations=EXCLUDED.has_locations, has_subscribers=EXCLUDED.has_subscribers, has_counterparts=EXCLUDED.has_counterparts;
        COMMIT;"""
    output_dir = os.path.join(output_root_dir, "sql", "syntheticdata")
    os.makedirs(output_dir, exist_ok=True)
    with open(f"{output_dir}/ingest_calls.sql", "w") as fout:
        fout.write("\n".join([ingest_head] + tables + [ingest_tail]))


if __name__ == "__main__":
    print("Generating synthetic data..")
    args = parser.parse_args()
    print(args)
    num_subscribers = args.n_subscribers
    num_cells = args.n_cells
    num_calls = args.n_calls

    subscriber_seed = args.subscribers_seed
    cell_seed = args.cells_seed
    call_seed = args.calls_seed

    output_root_dir = args.output_root_dir
    os.makedirs(os.path.join(output_root_dir, "data", "infrastructure"), exist_ok=True)

    print("Generating {} subscribers.".format(num_subscribers))
    sg = SubscriberGenerator()
    subscribers = list(sg.generate(num_subscribers, seed=subscriber_seed))

    print("Generating {} cells.".format(num_cells))
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

    print("Generating {} days of calls.".format(args.n_days))
    dates = pd.date_range(start="2016-01-01", periods=args.n_days)

    def write_f(seed_inc, date):
        return write_day_csv(
            subscribers, cells, date, num_calls, call_seed + seed_inc, output_root_dir
        )

    with ProcessPoolExecutor() as pool:
        tables = list(pool.map(write_f, *zip(*enumerate(dates))))
    write_ingest_sql(tables, output_root_dir)
