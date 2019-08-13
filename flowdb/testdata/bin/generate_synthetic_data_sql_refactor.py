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

# import matplotlib.pyplot as plt
import numpy as np
import random

import sqlalchemy as sqlalchemy
from sqlalchemy.exc import ResourceClosedError

import sys
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
    "--n-sites", type=int, default=10000, help="Number of sites to generate."
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
    "--n-mean_calls",
    type=int,
    default=50,
    help="Mean number of calls per subscriber to generate per day.",
)
parser.add_argument(
    "--n-sd_calls", type=int, default=5, help="Standard deviation for call creation."
)
parser.add_argument(
    "--n-mean_sms",
    type=int,
    default=50,
    help="Mean number of sms per subscriber to generate per day.",
)
parser.add_argument(
    "--n-sd_sms", type=int, default=5, help="Standard deviation for sms creation."
)
parser.add_argument(
    "--n-mean_mds",
    type=int,
    default=50,
    help="Mean number of mds per subscriber to generate per day.",
)
parser.add_argument(
    "--n-sd_mds", type=int, default=5, help="Standard deviation for mds creation."
)
parser.add_argument(
    "--n-startdate",
    type=int,
    default=1451606400,
    help="Timestamp of the day to start call data.",
)
parser.add_argument(
    "--dryrun",
    type=bool,
    default=False,
    help="Dry run the values to ensure call counts are correct.",
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

    # Ensure our distribution is fixed
    np.random.seed(42)

    # Generate a distribution for the total calls
    dist = np.random.normal(mu, sigma, size)

    # Find the min/max point of the distribution, and pass this into the histogram method
    start = int(round(min(dist)))
    end = int(round(max(dist)))
    subscribers, bins = np.histogram(dist, end - start, density=False)

    t = 0
    zero = 0
    output = {}
    count = start

    # Loop over the grouped subscribers to get the output
    for s in subscribers:
        if count <= 0:
            zero += s
        else:
            zero = 0

        if count >= 0:
            t += count * s  # The number of subscribers x the calls each
            output[count] = s + zero

        count += 1

    # The following will generate a plot (with smoothing) if needed to
    # test the ouput from the normal distribution generator
    if plot == True:
        plt.plot(bins, subscribers, linewidth=2, color="r")
        plt.savefig("nd.png")

    return output, t


# Generate distributed types
def generatedDistributedTypes(trans, dist, date, table, query):
    sql = []
    type_count = 0
    caller_id = 1
    to_id = 1
    vline = cycle(range(0, 50))
    callee_inc = round(num_subscribers * 0.1)

    # Create the SQL for outgoing/incoming SQL according to our distribution
    for d in dist:
        calls = d * dist[d]

        # If the call count is zero, then we can move on
        if calls <= 0:
            continue

        # Loop the number of subscribers in this "pot"
        from_count = 0
        to_count = 0
        for p in range(0, dist[d]):
            # Ensure we can generate the callee counts required
            if (to_id + 5) + (d * 2) - 1 >= num_subscribers:
                to_id = 1

            from_count = to_id + 5  # This value gives us a skip of subscribers
            to_count = from_count + (d * 2) - 1
            trans.execute(
                query.format(
                    table=table,
                    caller_id=caller_id,
                    from_count=from_count,
                    to_count=to_count,
                    timestamp=time.mktime(date.timetuple()),
                )
            )
            to_id += 1
            caller_id += 1


# Add post event SQL
def addEventSQL(type, table):
    sql = [
        f"CREATE INDEX ON events.{type}_{table} (msisdn);",
        f"CREATE INDEX ON events.{type}_{table} (tac);",
        f"CREATE INDEX ON events.{type}_{table} (location_id);",
        f"CREATE INDEX ON events.{type}_{table} (datetime);",
    ]

    # Only add the msisdn_counterpart index for calls and sms
    if type != "mds":
        sql.append(f"CREATE INDEX ON events.{type}_{table} (msisdn_counterpart);")

    sql.extend(
        [
            f"CLUSTER events.{type}_{table} USING {type}_{table}_msisdn_idx;",
            f"ANALYZE events.{type}_{table};",
            f"""
                INSERT INTO available_tables (table_name, has_locations, has_subscribers, has_counterparts) VALUES ('{type}', true, true, true)
                ON conflict (table_name)
                DO UPDATE SET has_locations=EXCLUDED.has_locations, has_subscribers=EXCLUDED.has_subscribers, has_counterparts=EXCLUDED.has_counterparts;
            """,
        ]
    )

    return sql


if __name__ == "__main__":
    args = parser.parse_args()
    with log_duration("Generating synthetic data..", **vars(args)):
        # Limit num_sites to 10000 due to geom.dat.
        num_sites = min(10000, args.n_sites)
        num_cells = args.n_cells
        num_tacs = args.n_tacs
        num_subscribers = args.n_subscribers
        num_days = args.n_days
        mean_calls = args.n_mean_calls
        sd_calls = args.n_sd_calls
        mean_sms = args.n_mean_sms
        sd_sms = args.n_sd_sms
        mean_mds = args.n_mean_mds
        sd_mds = args.n_sd_mds
        start_date = datetime.date.fromtimestamp(args.n_startdate)

        # Generate all the event distributions
        dist_calls, num_calls = generateNormalDistribution(
            num_subscribers, mean_calls, sd_calls
        )
        dist_sms, num_sms = generateNormalDistribution(
            num_subscribers, mean_sms, sd_sms
        )
        dist_mds, num_mds = generateNormalDistribution(
            num_subscribers, mean_mds, sd_mds
        )

        if args.dryrun:
            print(
                f"""
                With {num_subscribers} subscribers the output will be:
                
                calls -> a mean of {mean_calls} and sd of {sd_calls}: {num_calls * 2} calls rows per day {num_calls * 2 * num_days} total
                sms -> a mean of {mean_sms} and sd of {sd_sms}: {num_sms * 2} messages rows per day {num_sms * 2 * num_days} total
                mds -> a mean of {mean_mds} and sd of {sd_mds}, {num_mds} mds rows per day {num_mds * num_days} total
            """
            )
            sys.exit()

        engine = sqlalchemy.create_engine(
            f"postgresql://{os.getenv('POSTGRES_USER')}@/{os.getenv('POSTGRES_DB')}",
            echo=False,
            strategy="threadlocal",
            pool_size=cpu_count(),
            pool_timeout=None,
            isolation_level="AUTOCOMMIT",
        )

        connection = engine.connect()
        deferred_sql = []
        start_id = 1000000
        dir = os.path.dirname(os.path.abspath(__file__))

        # Main generation process
        with connection.begin() as trans:
            # Setup stage: Tidy up old event tables on previous runs
            with log_duration(job=f"Tidy up event tables"):
                tables = connection.execute(
                    "SELECT table_schema, table_name FROM information_schema.tables WHERE table_name ~ '^calls_|sms_|mds_'"
                ).fetchall()

                for t in tables:
                    connection.execute(f"DROP TABLE events.{t[1]};")

            # Steup stage 2. Load the variantions into a temp table
            with log_duration(job=f"Import variation data"):
                connection.execute(
                    f"""
                        CREATE TEMPORARY TABLE variations (
                            id SERIAL PRIMARY KEY,
                            row INT,
                            type TEXT,
                            value JSON
                        );
                        TRUNCATE variations;
                    """
                )

                for t in ["a", "b", "c", "d"]:
                    row = 1
                    with open(
                        f"{dir}/../synthetic_data/data/variations/{t}.dat", "r"
                    ) as f:
                        for l in f:
                            connection.execute(
                                f"""
                                    INSERT INTO variations (row, type, value) 
                                    VALUES ({row}, '{t}', '["{l.strip().replace(" ", '","')}"]');
                                """
                            )
                            row += 1

                    f.close()

            # The following generates the infrastructure schema data
            # 1. Sites and cells
            with log_duration(
                job=f"Generating {num_sites} sites with {num_cells} cell per site."
            ):
                with open(f"{dir}/../synthetic_data/data/geom.dat", "r") as f:
                    # First truncate the tables
                    connection.execute("TRUNCATE infrastructure.sites;")
                    connection.execute("TRUNCATE TABLE infrastructure.cells CASCADE;")

                    cell_id = start_id

                    # First create each site
                    for x in range(start_id, num_sites + start_id):
                        hash = generate_hash(x + 1000)
                        geom_point = f.readline().strip()

                        connection.execute(
                            f"""
                                INSERT INTO infrastructure.sites (id, version, date_of_first_service, date_of_last_service, geom_point) 
                                VALUES ('{hash}', 0, (date '{start_date}')::date, (date '{start_date}' + interval '{num_days} days')::date, '{geom_point}');
                            """
                        )

                        # And for each site, create n number of cells
                        for y in range(0, num_cells):
                            cellhash = generate_hash(cell_id)
                            connection.execute(
                                f"""
                                    INSERT INTO infrastructure.cells (id, version, site_id, date_of_first_service, date_of_last_service, geom_point) 
                                    VALUES ('{cellhash}', 0, '{hash}', (date '{start_date}')::date, (date '{start_date}' + interval '{num_days} days')::date, '{geom_point}');
                                """
                            )
                            cell_id += 1000

                    f.close()

            # 2. TACS
            with log_duration(f"Generating {num_tacs} tacs."):
                # First truncate the table
                connection.execute("TRUNCATE infrastructure.tacs;")
                # Then setup the temp sequences
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
                connection.execute(
                    f"""
                        CREATE TEMPORARY SEQUENCE brand MINVALUE 1 maxvalue {len(brands)} CYCLE;
                        CREATE TEMPORARY SEQUENCE hnd_type MINVALUE 1 maxvalue 3 CYCLE;
                    """
                )
                # Then run the inserts
                connection.execute(
                    f"""
                        INSERT INTO infrastructure.tacs (id, model, brand, hnd_type)
                        SELECT 
                        s.id * 10000 AS id,
                        md5((s.id + 10)::TEXT) AS  model,
                        (ARRAY['{"', '".join(brands)}'])[nextval('brand')] AS brand, 
                        (ARRAY['Smart', 'Feature', 'Basic'])[nextval('hnd_type')] AS hnd_type
                        FROM
                        (SELECT row_number() over() AS id
                        FROM generate_series(1, {num_tacs})) s;
                    """
                )

            # 3. Temporary subscribers
            with log_duration(f"Generating {num_subscribers} subscribers."):
                # Add the subscriber counts for variants
                weights = [0.35, 0.25, 0.25, 0.15]
                variants = ["a", "b", "c", "d"]
                variant_sql = "CASE "
                inc = 1
                for w, v in zip(weights, variants):
                    variant_sql += f"WHEN s.id BETWEEN {int(round(inc))}"
                    inc += w * num_subscribers
                    variant_sql += f" AND {int(round(inc) - 1)} THEN '{v}' "

                # Add the subscriber counts for tac brands - the weightings are interpreted from
                # here: https://www.statista.com/statistics/719123/share-of-cell-phone-brands-owned-in-the-uk/
                # to get the basic spread of ownsership of handset types
                weights = [0.02, 0.06, 0.38, 0.3, 0.07, 0.07, 0.06, 0.02, 0.02]
                tac_sql = "CASE "
                inc = 1
                for w, b in zip(weights, brands):
                    tac_sql += f"WHEN s.id BETWEEN {int(round(inc))}"
                    inc += w * num_subscribers
                    tac_sql += f" AND {int(round(inc) - 1)} THEN '{b}' "

                connection.execute(
                    f"CREATE TEMPORARY SEQUENCE brandcount MINVALUE 0 maxvalue {round(num_tacs / len(brands)) - 1} CYCLE;"
                )

                # Then insert the subscribers
                connection.execute(
                    f"""
                    CREATE TABLE subs as
                        SELECT s.id, md5((s.id + 10)::TEXT) AS msisdn, md5((s.id + 20)::TEXT) AS imei, md5((s.id + 30)::TEXT) AS imsi,
                        {variant_sql} END as variant,
                        (SELECT id FROM infrastructure.tacs where brand = ({tac_sql} END) 
                            OFFSET nextval('brandcount')
                            LIMIT 1
                        ) as tac
                        FROM
                        (SELECT row_number() over() AS id FROM generate_series(1, {num_subscribers})) s;
                """
                )

                # Add index and ANALYZE
                connection.execute(
                    """
                    CREATE INDEX on subs (id);
                    ANALYZE subs;
                """
                )

            trans.commit()

        # 4. Event SQL
        # Stores the PostgreSQL WITH statement to get subscribers
        with_sql = """
            WITH callers AS (
                SELECT 
                    s1.id AS id1, s2.id AS id2, s1.msisdn, s2.msisdn AS msisdn_counterpart, 
                    concat(v1.row, v2.row) as inc, v1.value AS caller_loc, v2.value AS callee_loc,
                    s1.imsi AS caller_imsi, s1.imei AS caller_imei, s1.tac AS caller_tac,
                    s2.imsi AS callee_imsi, s2.imei AS callee_imei, s2.tac AS callee_tac
                FROM subs s1
                    LEFT JOIN subs s2 
                    -- Series generator to provide the call count for each caller
                    on s2.id in (SELECT * FROM generate_series({from_count}, {to_count}, 2))
                    and s2.id != s1.id

                    LEFT JOIN variations v1
                    ON v1.type  = s1.variant
                    AND v1.row = (select nextval('rowcount'))

                    LEFT JOIN variations v2
                    ON v2.type  = s2.variant
                    AND v2.row = (select nextval('rowcount'))

                WHERE s1.id = {caller_id}
            )
        """
        # Create a temp rowcount/pointcount sequence to select the variation rows/points
        connection.execute(
            """
                CREATE TEMPORARY SEQUENCE rowcount MINVALUE 1 maxvalue 50 CYCLE;
                CREATE TEMPORARY SEQUENCE pointcount MINVALUE 0 maxvalue 99 CYCLE;
            """
        )
        # Loop over the days and generate all the types required
        for date in (start_date + datetime.timedelta(days=i) for i in range(num_days)):
            with connection.begin() as trans:
                # The date for the extended table names
                table = date.strftime("%Y%m%d")

                # 4.1 Calls
                if num_calls > 0:
                    with log_duration(f"Generating {num_calls} call events for {date}"):
                        # Create the table
                        connection.execute(
                            f"CREATE TABLE events.calls_{table} () INHERITS (events.calls);"
                        )

                        # Generate the distributed calls for this day
                        generatedDistributedTypes(
                            connection,
                            dist_calls,
                            date,
                            table,
                            with_sql
                            + """
                                INSERT INTO events.calls_{table} (id, outgoing, datetime, duration, msisdn, msisdn_counterpart, location_id, imsi, imei, tac) 
                                (
                                    SELECT
                                        md5(concat({timestamp}, id)) AS id, outgoing,
                                        '{table}'::TIMESTAMPTZ + interval '1 seconds' * (datetime / 2) AS datetime,
                                        floor(0.5 * 2600) AS duration,
                                        msisdn, msisdn_counterpart,
                                        (SELECT id FROM infrastructure.cells where ST_Equals(geom_point,loc::geometry)) AS location_id,
                                        imsi, imei, tac
                                    FROM (
                                        SELECT CONCAT(id1::text, inc, id2::text) AS id, id1 AS datetime, true AS outgoing, msisdn, msisdn_counterpart, 
                                        caller_loc->>nextval('pointcount')::INTEGER AS loc, caller_imsi AS imsi, caller_imei AS imei, caller_tac AS tac from callers
                                        UNION ALL
                                        SELECT CONCAT(id2::text, (inc::INTEGER * id2), id1::text) AS id, id1 AS datetime, false AS outgoing, msisdn_counterpart AS msisdn, 
                                        msisdn AS msisdn_counterpart, callee_loc->>nextval('pointcount')::INTEGER AS loc, callee_imsi AS imsi, callee_imei AS imei, 
                                        callee_tac AS tac from callers
                                    ) _
                                )
                            """,
                        )

                    # Add the indexes for this day
                    deferred_sql.append(
                        (
                            f"Adding table analyzing for calls_{table}",
                            addEventSQL("calls", table),
                        )
                    )

                # 4.2 SMS
                if num_sms > 0:
                    with log_duration(f"Generating {num_sms} sms events for {date}"):
                        connection.execute(
                            f"CREATE TABLE IF NOT EXISTS events.sms_{table} () INHERITS (events.sms);"
                        )

                        # Generate the distributed sms for this day
                        generatedDistributedTypes(
                            connection,
                            dist_sms,
                            date,
                            table,
                            with_sql
                            + """
                                INSERT INTO events.sms_{table} (id, outgoing, datetime, msisdn, msisdn_counterpart, location_id, imsi, imei, tac) 
                                (
                                    SELECT
                                        md5(concat({timestamp}, id)) AS id, outgoing,
                                        '{table}'::TIMESTAMPTZ + interval '1 seconds' * (datetime / 2) AS datetime,
                                        msisdn, msisdn_counterpart,
                                        (SELECT id FROM infrastructure.cells where ST_Equals(geom_point,loc::geometry)) AS location_id,
                                        imsi, imei, tac
                                    FROM (
                                        SELECT CONCAT(id1::text, inc, id2::text) AS id, id1 AS datetime, true AS outgoing, msisdn, msisdn_counterpart, 
                                        caller_loc->>nextval('pointcount')::INTEGER AS loc, caller_imsi AS imsi, caller_imei AS imei, caller_tac AS tac from callers
                                        UNION ALL
                                        SELECT CONCAT(id2::text, (inc::INTEGER * id2), id1::text) AS id, id1 AS datetime, false AS outgoing, msisdn_counterpart AS msisdn, 
                                        msisdn AS msisdn_counterpart, callee_loc->>nextval('pointcount')::INTEGER AS loc, callee_imsi AS imsi, callee_imei AS imei, 
                                        callee_tac AS tac from callers
                                    ) _
                                )
                            """,
                        )

                    # Add the indexes for this day
                    deferred_sql.append(
                        (
                            f"Adding table analyzing for sms_{table}",
                            addEventSQL("sms", table),
                        )
                    )

                # 4.3 MDS
                if num_mds > 0:
                    with log_duration(f"Generating {num_mds} mds events for {date}"):
                        connection.execute(
                            f"CREATE TABLE IF NOT EXISTS events.mds_{table} () INHERITS (events.mds);"
                        )

                        # Generate the distributed mds for this day
                        generatedDistributedTypes(
                            connection,
                            dist_mds,
                            date,
                            table,
                            """
                                WITH callers AS (
                                    SELECT s.msisdn, s.imei, s.imsi, s.tac, v.value as loc, round(0.5 * 100000) as volume
                                    FROM subs s
                                        
                                        LEFT JOIN variations v
                                        ON v.type  = s.variant
                                        AND v.row = (select nextval('rowcount'))
                                    
                                    WHERE s.id = {caller_id}
                                )
                                INSERT INTO events.mds_{table} (id, datetime, duration, volume_total, volume_upload, volume_download, msisdn, imei, imsi, tac, location_id) (
                                    SELECT 
                                    MD5(CONCAT({timestamp}, s.id, c.msisdn)) AS id,
                                    '{table}'::TIMESTAMPTZ + interval '1 seconds' * (point / 3) AS datetime,
                                    FLOOR(0.5 * 2600) AS duration,
                                    c.volume * 2 AS volume_total,
                                    c.volume AS volume_upload,
                                    c.volume AS volume_download,
                                    c.msisdn, c.imei, c.imsi, c.tac, (SELECT id FROM infrastructure.cells where ST_Equals(geom_point, (c.loc->>s.point::INTEGER)::geometry)) AS loc
                                    FROM
                                    callers c,
                                    (SELECT row_number() over() AS id, nextval('pointcount') AS point FROM generate_series({from_count}, {to_count}, 2)) s
                                )
                            """,
                        )

                    # Add the indexes for this day
                    deferred_sql.append(
                        (
                            f"Adding table analyzing for mds_{table}",
                            addEventSQL("mds", table),
                        )
                    )

                trans.commit()

        # Add all the ANALYZE calls for the events tables.
        deferred_sql.append(
            (
                "Analyzing the main event tables",
                ["ANALYZE events.calls;", "ANALYZE events.sms;", "ANALYZE events.mds;"],
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
