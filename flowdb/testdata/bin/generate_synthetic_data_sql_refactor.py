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
import matplotlib.pyplot as plt
import numpy as np
import random

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
    "--n-sms", type=int, default=200_000, help="Number of sms to generate per day."
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
def generateNormalDistribution(size, mu=0, sigma=1, plot=False):
    """
    Generates a normal distributed progression for use in seeding
    data. If mu/sigma aren't passed in, they will default to 0/1
    
    It is also possible to generate a smoothed plotted output to 
    test that the values hit the normal distribution requirements
    """

    s = np.random.normal(mu, sigma, size)

    # The following will generate a plot (with smoothing) if needed to
    # test the ouput from the normal distribution generator
    if plot == True:
        pdf, bins = np.histogram(s, 10, density=True)
        bins = np.delete(bins, 1, 0)

        plt.plot(bins, pdf, linewidth=2, color="r")
        plt.savefig("nd.png")

    return s


# Generate distributed types
def generatedDistributedTypes(type, num_type, date, table, query):
    sql = []

    # Get a distribution
    dist = generateNormalDistribution(num_subscribers, num_type / num_subscribers, 2)

    type_count = 1
    offset = 0
    vline = cycle(range(0, 50))

    # Create the SQL for outgoing/incoming SQL according to our distribution
    for d in dist:
        count = int(round(d))
        if count <= 0:
            continue

        # Get the caller, and variant
        caller = trans.execute(
            f"SELECT *, ('{table}'::TIMESTAMPTZ + random() * interval '1 day') AS date FROM subs LIMIT 1 OFFSET {offset}"
        ).first()
        # Select the caller variant
        callervariant = variants[caller[5]][next(vline)]

        for c in range(0, count):
            # Get callee rows - TODO: better appraoch to selection
            callee = trans.execute(
                f"SELECT * FROM subs WHERE id != {caller[0]} ORDER BY RANDOM() LIMIT 1 "
            ).first()
            # Select the calleevariant
            calleevariant = variants[callee[5]][next(vline)]

            # Set a duration and hashes
            duration = floor(random.random() * 2600)
            outgoing = generate_hash(time.mktime(date.timetuple()) + type_count)
            incoming = generate_hash(
                time.mktime(date.timetuple()) + count + c + type_count
            )

            # Generate the outgoing/incoming of type
            sql.append(
                query.format(
                    table=table,
                    caller=caller,
                    callee=callee,
                    outgoing=outgoing,
                    incoming=incoming,
                    callervariant=callervariant[int(c % d)],
                    calleevariant=calleevariant[int(c % d)],
                )
            )

            type_count += 1
            offset += 1

            if type_count >= num_type:
                break
            if offset >= num_subscribers:
                offset = 0

        else:
            continue

        break

    return sql


# Add post event SQL
def addEventSQL(type, table):
    return [
        f"CREATE INDEX ON events.{type}_{table} (msisdn);",
        f"CREATE INDEX ON events.{type}_{table} (msisdn_counterpart);",
        f"CREATE INDEX ON events.{type}_{table} (tac);",
        f"CREATE INDEX ON events.{type}_{table} (location_id);",
        f"CREATE INDEX ON events.{type}_{table} (datetime);",
        f"CLUSTER events.{type}_{table} USING {type}_{table}_msisdn_idx;",
        f"ANALYZE events.{type}_{table};",
    ]


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
        num_sms = args.n_sms
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
            # Setup stage: Tidy up old event tables on previous runs
            with log_duration(job=f"Tidy up event tables"):
                tables = trans.execute(
                    "SELECT table_schema, table_name FROM information_schema.tables WHERE table_name ~ '^calls_|sms_|mds_'"
                ).fetchall()

                for t in tables:
                    trans.execute(f"DROP TABLE events.{t[1]};")

            # The following generates the infrastructure schema data
            # 1. Sites and cells
            with log_duration(
                job=f"Generating {num_sites} sites with {num_cells} cell per site."
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
                    line.strip().split(" ", 100)
                    for line in open(
                        f"{dir}/../synthetic_data/data/variations/{t}.dat", "r"
                    )
                ]

            # Loop over the days and generate all the types required
            for date in (
                start_date + datetime.timedelta(days=i) for i in range(num_days)
            ):
                table = date.strftime("%Y%m%d")

                # 4.1 Calls
                if num_calls > 0:
                    call_sql = [
                        f"CREATE TABLE IF NOT EXISTS events.calls_{table} () INHERITS (events.calls);",
                        f"TRUNCATE events.calls_{table};",
                    ]

                    # Get a distribution for this day
                    dist = generateNormalDistribution(
                        num_subscribers, num_calls / num_subscribers, 2
                    )

                    calls = 1
                    offset = 0
                    vline = cycle(range(0, 50))

                    # Create the SQL for outgoing/incoming SQL according to our distribution
                    for d in dist:
                        call_count = int(round(d))
                        if call_count <= 0:
                            continue

                        # Get the caller, and variant
                        caller = trans.execute(
                            f"SELECT *, ('{table}'::TIMESTAMPTZ + random() * interval '1 day') AS date FROM subs LIMIT 1 OFFSET {offset}"
                        ).first()
                        # Select the caller variant
                        callervariant = variants[caller[5]][next(vline)]

                        for c in range(0, call_count):
                            # Get callee rows - TODO: better appraoch to selection
                            callee = trans.execute(
                                f"SELECT * FROM subs WHERE id != {caller[0]} ORDER BY RANDOM() LIMIT 1 "
                            ).first()
                            # Select the calleevariant
                            calleevariant = variants[callee[5]][next(vline)]

                            # Set a duration and hashes
                            duration = floor(random.random() * 2600)
                            outgoing = generate_hash(
                                time.mktime(date.timetuple()) + calls
                            )
                            incomming = generate_hash(
                                time.mktime(date.timetuple()) + call_count + c + calls
                            )

                            # Generate the outgoing/incoming calls
                            call_sql.append(
                                f""" 
                                    INSERT INTO events.calls_{table} (
                                        id, datetime, outgoing, msisdn, msisdn_counterpart, imei, imsi, tac, duration, location_id
                                    ) VALUES 
                                    (
                                        '{outgoing}', '{caller[6]}', true, '{caller[1]}', '{callee[1]}', '{caller[2]}',
                                        '{caller[3]}', '{caller[4]}', {duration}, 
                                        (SELECT id FROM infrastructure.cells WHERE ST_Equals(geom_point, '{callervariant[int(c % d)]}'::geometry) LIMIT 1)
                                    );
                                    INSERT INTO events.calls_{table} (
                                        id, datetime, outgoing, msisdn, msisdn_counterpart, imei, imsi, tac, duration, location_id
                                    ) VALUES 
                                    (
                                        '{incomming}', '{caller[6]}', false, '{callee[1]}', '{caller[1]}', '{callee[2]}',
                                        '{callee[3]}', '{callee[4]}', {duration},
                                        (SELECT id FROM infrastructure.cells WHERE ST_Equals(geom_point, '{calleevariant[int(c % d)]}'::geometry) LIMIT 1)
                                    );
                                """
                            )

                            calls += 1
                            offset += 1

                            if calls >= num_calls:
                                break
                            if offset >= num_subscribers:
                                offset = 0

                        else:
                            continue

                        break

                    # Add the indexes for this day
                    call_sql.extend(addEventSQL("calls", table))
                    deferred_sql.append(
                        (f"Generating {num_calls} call events for {date}", call_sql)
                    )

                # 4.2 SMS
                if num_sms > 0:
                    sms_sql = [
                        f"CREATE TABLE IF NOT EXISTS events.sms_{table} () INHERITS (events.sms);",
                        f"TRUNCATE events.sms_{table};",
                    ]

                    # Generate the distributed sms for this day
                    sms_sql.extend(
                        generatedDistributedTypes(
                            "sms",
                            num_sms,
                            date,
                            table,
                            """
                        INSERT INTO events.sms_{table} (
                            id, datetime, outgoing, msisdn, msisdn_counterpart, imei, imsi, tac, location_id
                        ) VALUES 
                        (
                            '{outgoing}', '{caller[6]}', true, '{caller[1]}', '{callee[1]}', '{caller[2]}',
                            '{caller[3]}', '{caller[4]}',
                            (SELECT id FROM infrastructure.cells WHERE ST_Equals(geom_point, '{callervariant}'::geometry) LIMIT 1)
                        );
                        INSERT INTO events.sms_{table} (
                            id, datetime, outgoing, msisdn, msisdn_counterpart, imei, imsi, tac, location_id
                        ) VALUES 
                        (
                            '{incoming}', '{caller[6]}', false, '{callee[1]}', '{caller[1]}', '{callee[2]}',
                            '{callee[3]}', '{callee[4]}',
                            (SELECT id FROM infrastructure.cells WHERE ST_Equals(geom_point, '{calleevariant}'::geometry) LIMIT 1)
                        );
                    """,
                        )
                    )

                    # Add the indexes for this day
                    sms_sql.extend(addEventSQL("sms", table))
                    deferred_sql.append(
                        (f"Generating {num_sms} sms events for {date}", sms_sql)
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
