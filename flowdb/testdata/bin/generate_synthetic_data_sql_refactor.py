# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# !/usr/bin/env python

"""
Small script for generating arbitrary volumes of CDR call data inside the flowdb
container.

Produces sites, cells, tacs, call, sms and mds data.

Optionally simulates a 'disaster' where all subscribers must leave a designated admin2 region
for a period of time.
"""

import os
import argparse
import datetime
import time
from hashlib import md5
from concurrent.futures.thread import ThreadPoolExecutor
from contextlib import contextmanager
from multiprocessing import cpu_count
from itertools import cycle
from math import floor
import numpy as np

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
parser = argparse.ArgumentParser(description="Flowminder Synthetic CDR Generator\n")

parser.add_argument(
    "--n-sites", type=int, default=1000, help="Number of sites to generate."
)
parser.add_argument(
    "--n-cells", type=int, default=1, help="Number of cells to generate per site."
)
parser.add_argument(
    "--n-tacs", type=int, default=2000, help="Number of phone models to generate."
)
parser.add_argument(
    "--n-subscribers", type=int, default=4000, help="Number of subscribers to generate."
)
parser.add_argument(
    "--n-days", type=int, default=7, help="Number of days of data to generate."
)
parser.add_argument(
    "--n-calls", type=int, default=200_000, help="Number of calls to generate per day."
)
parser.add_argument(
    "--n-startdate",
    type=int,
    default=1451606400,
    help="Timestamp of the day to start call data.",
)

# Logging context
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


# Generate MD5 HashDigest
def generate_hash(index):
    """
    Generates a md5 checksum from an integer index value
    """
    return md5(int(index).to_bytes(8, "big", signed=True)).hexdigest()


# Generate normal distribution
def generateNormalDistribution(size, mu=0, sigma=1):
    """
    Generates a normal distributed progression for use in seeding
    data. If mu/sigma aren't passed in, they will default to 0/1
    """

    # Fix the seend to ensure data is the same
    np.random.seed(42)

    return np.random.normal(mu, sigma, size)


if __name__ == "__main__":
    args = parser.parse_args()
    with log_duration("Generating synthetic data..", **vars(args)):
        # Limit num_sites to 10000 due to geom.dat.
        num_sites = min(10000, args.n_sites)
        num_cells = args.n_cells
        num_tacs = args.n_tacs
        num_subscribers = args.n_subscribers
        num_days = args.n_days
        num_calls = args.n_calls
        start_date = datetime.date.fromtimestamp(args.n_startdate)

        engine = sqlalchemy.create_engine(
            f"postgresql://{os.getenv('POSTGRES_USER')}@/{os.getenv('POSTGRES_DB')}",
            echo=False,
            strategy="threadlocal",
            pool_size=cpu_count(),
            pool_timeout=None,
        )

        deferred_sql = []
        start_id = 1000000
        dir = os.path.dirname(os.path.abspath(__file__))

        # Main generation process
        with engine.begin() as trans:

            # The following generates the infrastructure schema data
            # 1. Sites and cells
            with log_duration(
                job=f"Generating {num_sites} sites and {num_cells} cells."
            ):
                with open(f"{dir}/../synthetic_data/data/geom.dat", "r") as f:
                    # First truncate the tables
                    trans.execute("TRUNCATE infrastructure.sites;")
                    trans.execute("TRUNCATE TABLE infrastructure.cells CASCADE;")

                    cell_id = start_id

                    # First create each site
                    for x in range(start_id, num_sites + start_id):
                        hash = generate_hash(x + 1000)
                        geom_point = f.readline().strip()

                        trans.execute(
                            f"""
                                INSERT INTO infrastructure.sites (id, version, date_of_first_service, geom_point) 
                                VALUES ('{hash}', 0, (date '2015-01-01' + random() * interval '1 year')::date, '{geom_point}');
                            """
                        )

                        # And for each site, create n number of cells
                        for y in range(0, num_cells):
                            cellhash = generate_hash(cell_id)
                            trans.execute(
                                f"""
                                    INSERT INTO infrastructure.cells (id, version, site_id, date_of_first_service, geom_point) 
                                    VALUES ('{cellhash}', 0, '{hash}', (date '2015-01-01' + random() * interval '1 year')::date, '{geom_point}');
                                """
                            )
                            cell_id += 1000

                    f.close()

            # 2. TACS
            with log_duration(f"Generating {num_tacs} tacs."):
                # First truncate the table
                trans.execute("TRUNCATE infrastructure.tacs;")
                brands = [
                    "Nokia",
                    "Huawei",
                    "Apple",
                    "Samsung",
                    "Sony",
                    "LG",
                    "Google",
                    "Xiaomi",
                    "ZTE",
                ]
                types = ["Smart", "Feature", "Basic"]

                # Create cycles to loop over the types/brands.
                brand = cycle(brands)
                type = cycle(types)
                for x in range(start_id, num_tacs + start_id):
                    id = x + 1000
                    hash = generate_hash(id)
                    trans.execute(
                        f"""
                            INSERT INTO infrastructure.tacs (id, brand, model, hnd_type) VALUES ({id}, '{next(brand)}', '{hash}', '{next(type)}');
                        """
                    )

            # 3. Temporary subscribers
            with log_duration(f"Generating {num_subscribers} subscribers."):
                trans.execute(
                    f"""
                        CREATE TABLE IF NOT EXISTS subs (
                            id SERIAL PRIMARY KEY,
                            msisdn TEXT,
                            imei TEXT,
                            imsi TEXT,
                            tac INT,
                            variant TEXT
                        );
                        TRUNCATE subs;
                    """
                )
                # These are interpreted from here: https://www.statista.com/statistics/719123/share-of-cell-phone-brands-owned-in-the-uk/
                # to get the basic spread of ownsership of handset types
                tacWeights = [0.02, 0.06, 0.38, 0.3, 0.07, 0.07, 0.06, 0.02, 0.02]

                # These are the variants generated by generate_geom script - these will be loaded to generate the calls
                variantWeights = [0.35, 0.25, 0.25, 0.15]
                variants = ["a", "b", "c", "d"]

                tacWeight = cycle(tacWeights)
                variantWeight = cycle(variantWeights)

                b = 0
                i = 0
                t = next(tacWeight) * num_subscribers
                v = next(variantWeight) * num_subscribers

                for x in range(start_id, num_subscribers + start_id):
                    msisdn = generate_hash(x + 1000)
                    imei = generate_hash(x + 2000)
                    imsi = generate_hash(x + 3000)

                    if t == 0:
                        t = next(tacWeight) * num_subscribers
                        b += 1

                    if v == 0:
                        v = next(variantWeight) * num_subscribers
                        i += 1

                    trans.execute(
                        f"""
                            INSERT INTO subs (msisdn, imei, imsi, tac, variant) VALUES ('{msisdn}','{imei}','{imsi}', 
                            (SELECT id FROM infrastructure.tacs where brand = '{brands[b]}' ORDER BY RANDOM() LIMIT 1), 
                            '{variants[i]}');
                        """
                    )

                    t -= 1
                    v -= 1

            # 4. Event SQL
            variants = {}
            for t in ["a", "b", "c", "d"]:
                variants[t] = [
                    line.strip()
                    for line in open(
                        f"{dir}/../synthetic_data/data/variations/{t}.dat", "r"
                    )
                ]

            # Loop over the days required
            for date in (
                start_date + datetime.timedelta(days=i) for i in range(num_days)
            ):
                table = date.strftime("%Y%m%d")

                # 4.1 Calls
                if num_calls > 0:
                    # TODO - remove existing days so we get an exact data set
                    call_sql = [
                        f"CREATE TABLE IF NOT EXISTS events.calls_{table} () INHERITS (events.calls);",
                        f"TRUNCATE events.calls_{table};",
                    ]

                    for x in range(0, num_calls):
                        hash = generate_hash(time.mktime(date.timetuple()) + x)
                        call_sql.append(
                            f""" 
                                INSERT INTO events.calls_{table} (id, datetime, outgoing, msisdn) 
                                VALUES 
                                ('{hash}', true, ('{table}'::TIMESTAMPTZ + random() * interval '1 day'), '{hash}')
                            """
                        )

                    # Add the indexes for this day
                    call_sql.append(f"CREATE INDEX ON events.calls_{table} (msisdn);")
                    call_sql.append(
                        f"CREATE INDEX ON events.calls_{table} (msisdn_counterpart);"
                    )
                    call_sql.append(f"CREATE INDEX ON events.calls_{table} (tac);")
                    call_sql.append(
                        f"CREATE INDEX ON events.calls_{table} (location_id);"
                    )
                    call_sql.append(f"CREATE INDEX ON events.calls_{table} (datetime);")
                    call_sql.append(
                        f"CLUSTER events.calls_{table} USING calls_{table}_msisdn_idx;"
                    )
                    call_sql.append(f"ANALYZE events.calls_{table};")

                    deferred_sql.append(
                        (f"Generating {num_calls} call events for {date}", call_sql)
                    )

            # Add all the ANALYZE calls for the events tables.
            deferred_sql.append(
                (
                    "Analyzing the events tables",
                    [
                        "ANALYZE events.calls;",
                        "ANALYZE events.sms;",
                        "ANALYZE events.mds;",
                    ],
                )
            )

        # Remove the intermediary data tables
        for tbl in ("subs",):
            deferred_sql.append((f"Dropping {tbl}", [f"DROP TABLE {tbl};"]))

        def do_exec(args):
            msg, sql = args
            with log_duration(msg):
                with engine.begin() as trans:
                    for s in sql:
                        res = trans.execute(s)
                        try:
                            logger.info(f"SQL result", job=msg, result=res.fetchall())
                        except ResourceClosedError:
                            pass  # Nothing to do here

        with ThreadPoolExecutor(cpu_count()) as tp:
            list(tp.map(do_exec, deferred_sql))
