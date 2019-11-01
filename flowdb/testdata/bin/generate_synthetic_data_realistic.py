# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# !/usr/bin/env python

"""
A script for the generation of realistic and reproducible CDR data (based on a 
normal distribution of movement and call volumes per subscriber) inside of the 
flowdb container for n number of days.

It starts by producing sites & cells according to the locations set in the 
data/geom.dat file to ensure that they are always set according to specific fixed 
locations. 

In producing tacs data, these are distributed accordingly between a list of brands
to ensure a realistic distribution of handset data. A temporary list of subscribers 
is also produced - each given a defined msisdn, imei & imsi. This is used as a 
reference for all event data and removed once the seed process has completed.

The volume of events per day for each type is determined by setting its mean and
standard deviation - e.g. MEAN_CALLS and SD_CALLS respectively fo call events. These
settings will result in a normally distributed pattern of calls across the defined
number of subscribers along with the movement of each subscriber defined by the 
data defined in the folder data/variations/*.dat. 

It is possible to run the script in 'dryrun' mode to show the volumes of events of 
each type that will be created by the defined args provided to the script. In this 
way it's possible to define a combination of mean and sd for each event type that
will give the volumes of event data required. 
"""

import os
import argparse
import datetime
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
def generatedDistributedTypes(trans, dist, date, date_sk, query):
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
                    date_sk=date_sk,
                    caller_id=caller_id,
                    from_count=from_count,
                    to_count=to_count,
                    date=date,
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
            # Setup stage: Tidy up subscriber_sightings generated on previous runs
            with log_duration(
                job=f"Tidy up subscriber_sightings, event_supertable etc and date_dim tables"
            ):
                connection.execute(
                    """
                    TRUNCATE TABLE interactions.date_dim RESTART IDENTITY CASCADE;
                    TRUNCATE TABLE interactions.subscriber_sightings RESTART IDENTITY CASCADE;
                    TRUNCATE TABLE interactions.event_supertable RESTART IDENTITY CASCADE;
                    TRUNCATE TABLE interactions.calls RESTART IDENTITY CASCADE;
                    TRUNCATE TABLE interactions.sms RESTART IDENTITY CASCADE;
                    TRUNCATE TABLE interactions.mds RESTART IDENTITY CASCADE;
                """
                )
                tables = connection.execute(
                    "SELECT table_schema, table_name FROM information_schema.tables WHERE table_name ~ '(subscriber_sightings|event_supertable|calls|sms|mds)_[0-9]+'"
                ).fetchall()

                for t in tables:
                    connection.execute(f"DROP TABLE {t[0]}.{t[1]};")

            # Setup stage 2. Load the variantions into a temp table
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

            # Setup stage 3. Create time dimension table
            with log_duration(job=f"Populate the time_dimension table"):
                # First truncate the tables
                connection.execute("TRUNCATE interactions.time_dimension CASCADE;")
                connection.execute(
                    f"""
                        INSERT INTO interactions.time_dimension (time_sk, hour)
	                        SELECT id, id - 1 FROM (SELECT ROW_NUMBER() OVER() AS id FROM generate_series(1, 24, 1)) _
                    """
                )

            # The following generates the infrastructure & interactions schema data
            # 1. Sites and cells
            with log_duration(
                job=f"Generating {num_sites} sites/locations & populating geo_bridge."
            ):
                with open(f"{dir}/../synthetic_data/data/geom.dat", "r") as f:
                    # First truncate the tables
                    connection.execute("TRUNCATE infrastructure.sites;")
                    connection.execute("TRUNCATE geography.geo_bridge;")
                    connection.execute("TRUNCATE TABLE interactions.locations CASCADE;")

                    cell_id = start_id

                    # First create each site
                    for x in range(1, num_sites + 1):
                        hash = generate_hash(x + 1000)
                        geom_point = f.readline().strip()

                        # Insert sites
                        connection.execute(
                            f"""
                                INSERT INTO infrastructure.sites (id, version, date_of_first_service, date_of_last_service, geom_point) 
                                VALUES ('{hash}', 0, (date '{start_date}')::date, (date '{start_date}' + interval '{num_days} days')::date, '{geom_point}');
                            """
                        )

                        # Insert locations
                        connection.execute(
                            f"""
                                INSERT INTO interactions.locations (cell_id, site_id, "position") 
                                VALUES ({x}, '{hash}', '{geom_point}');
                            """
                        )

                        # Populate the geo_bridge table
                        for y in ["admin1", "admin2", "admin3"]:
                            connection.execute(
                                f"""
                                    INSERT INTO geography.geo_bridge (cell_id, geo_id, valid_from, valid_to, linkage_method) (
                                        SELECT {x}, gid, (date '{start_date}')::date, (date '{start_date}' + interval '{num_days} days')::date, '{y}' FROM geography.{y}
                                        WHERE ST_WITHIN('{geom_point}'::GEOMETRY, ST_SETSRID(geom, 4326)::GEOMETRY)); 
                                """
                            )

                    f.close()

                    # Analyze the location tables
                    deferred_sql.append(
                        (
                            "Analyzing the location and geo_bridge tables",
                            [
                                "ANALYZE geography.geo_bridge;",
                                "ANALYZE interactions.locations;",
                            ],
                        )
                    )

            # 2. TACS
            with log_duration(f"Generating {num_tacs} tacs."):
                # First truncate the table
                connection.execute("TRUNCATE infrastructure.tacs CASCADE;")
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

            # 3. Subscribers
            with log_duration(f"Generating {num_subscribers} subscribers."):
                # First truncate the table
                connection.execute("TRUNCATE TABLE interactions.subscriber CASCADE;")

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

                # Insert the temp subscribers with variant data
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

                # Copy only the relevant fields to interactions.subscriber
                connection.execute(
                    """
                    INSERT INTO interactions.subscriber (id, msisdn, imei, imsi, tac)
                        SELECT id, msisdn, imei, imsi, tac from subs;
                """
                )

                # Add index and ANALYZE
                connection.execute(
                    """
                    CREATE INDEX on subs (id);
                    ANALYZE subs;
                """
                )

                # Analyze subscribers
                deferred_sql.append(
                    (
                        "Analyzing the subscriber table",
                        ["ANALYZE interactions.subscriber;"],
                    )
                )

            trans.commit()

        # 4. Subscriber sightings fact data
        # Stores the PostgreSQL WITH statement to get subscribers
        with_sql = """
            WITH callers AS (
                SELECT 
                    s1.id AS id1, s2.id AS id2, v1.value->>NEXTVAL('pointcount')::INTEGER AS caller_loc, 
                    v2.value->>NEXTVAL('pointcount')::INTEGER AS callee_loc,
                    NEXTVAL('time_dimension') AS time_sk, 
                    CONCAT('{date} ', LPAD((currval('time_dimension') - 1)::TEXT, 2, '0'), 
                        ':', LPAD(NEXTVAL('minutes')::TEXT, 2, '0'), 
                        ':', LPAD(NEXTVAL('seconds')::TEXT, 2, '0')
                    )::TIMESTAMPTZ as "timestamp",
                    s1.msisdn AS calling_party_msisdn, 
                    s2.msisdn AS called_party_msisdn,
                    FLOOR(CAST(nextval('duration') as FLOAT) / 10 * 2600)  AS duration
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
        # Create a temp sequences to select the variation and generate times
        connection.execute(
            """
                CREATE TEMPORARY SEQUENCE rowcount MINVALUE 1 maxvalue 50 CYCLE;
                CREATE TEMPORARY SEQUENCE pointcount MINVALUE 0 maxvalue 99 CYCLE;
                CREATE TEMPORARY SEQUENCE time_dimension MINVALUE 1 maxvalue 24 CYCLE;
                CREATE TEMPORARY SEQUENCE minutes MINVALUE 0 maxvalue 59 CYCLE;
                CREATE TEMPORARY SEQUENCE seconds MINVALUE 0 maxvalue 59 cycle increment -1;
                CREATE TEMPORARY SEQUENCE duration MINVALUE 1 maxvalue 10 cycle;
            """
        )
        # Loop over the days and generate all the types required
        for i in range(num_days):
            with connection.begin() as trans:
                # The date for the extended table names
                date = start_date + datetime.timedelta(days=i)

                # 4.1 Create the date_dim row
                connection.execute(
                    f"""
                    INSERT INTO interactions.date_dim (date_sk, date, day_of_week, day_of_month, year) VALUES (
                        {i + 1}, DATE('{date}'), TO_CHAR(DATE('{date}'), 'day'), DATE_PART('day', DATE('{date}')), 
                        DATE_PART('year', DATE('{date}')))
                      """
                )

                # 4.2 Create the subscriber_sightings partition
                connection.execute(
                    f"""
                    CREATE TABLE interactions.subscriber_sightings_{str(i + 1).rjust(5, '0')} 
                        PARTITION OF interactions.subscriber_sightings FOR VALUES IN ({i + 1});
                    CREATE TABLE interactions.event_supertable_{str(i + 1).rjust(5, '0')} 
                        PARTITION OF interactions.event_supertable FOR VALUES IN ({i + 1});
                    CREATE TABLE interactions.calls_{str(i + 1).rjust(5, '0')} 
                        PARTITION OF interactions.calls FOR VALUES IN ({i + 1});
                """
                )

                # 4.2 Calls
                if num_calls > 0:
                    with log_duration(f"Generating {num_calls} call events for {date}"):
                        # Generate the distributed calls for this day
                        generatedDistributedTypes(
                            connection,
                            dist_calls,
                            date,
                            (i + 1),
                            with_sql
                            + """
                                , event_supertable AS (
                                    INSERT INTO interactions.event_supertable (subscriber_id, cell_id, date_sk, time_sk, event_type, "timestamp")
                                        SELECT id1 AS subscriber_id, (SELECT cell_id FROM interactions.locations where ST_Equals("position", caller_loc::geometry)) AS cell_id, 
                                        {date_sk} AS date_sk, time_sk, 1 AS event_type, "timestamp" FROM callers
                                        RETURNING *
                                ), unions AS (
                                    SELECT id1 as subscriber_id, caller_loc AS loc, time_sk, "timestamp" from callers
                                    UNION ALL
                                    SELECT id2 as subscriber_id, callee_loc AS loc, time_sk, "timestamp" from callers
                                ), calls AS (
                                    INSERT INTO interactions.calls (super_table_id, subscriber_id, called_subscriber_id, calling_party_cell_id, called_party_cell_id, date_sk, time_sk, calling_party_msisdn, called_party_msisdn, call_duration, "timestamp")
                                        select event_id, subscriber_id, id2, cell_id, (SELECT cell_id FROM interactions.locations where ST_Equals("position", callee_loc::geometry)), e.date_sk, e.time_sk, calling_party_msisdn, called_party_msisdn, duration, e."timestamp" from event_supertable e
                                        JOIN callers _ USING("timestamp")
                                        RETURNING *
                                )
                                INSERT INTO interactions.subscriber_sightings (subscriber_id, cell_id, date_sk, time_sk, event_super_table_id, "timestamp") 
                                    select _.subscriber_id, (SELECT cell_id FROM interactions.locations where ST_Equals("position",loc::geometry)) AS cell_id, c.date_sk, _.time_sk, super_table_id, "timestamp" from calls c
                                    JOIN UNIONS _ using("timestamp")
                            """,
                        )

                # 4.3 SMS
                if num_sms > 0:
                    with log_duration(f"Generating {num_sms} sms events for {date}"):
                        # Generate the distributed sms for this day
                        generatedDistributedTypes(
                            connection,
                            dist_sms,
                            date,
                            (i + 1),
                            with_sql
                            + """
                                , event_supertable as (
                                    INSERT INTO interactions.event_supertable (subscriber_id, cell_id, date_sk, time_sk, event_type, "timestamp")
                                        SELECT id1 AS subscriber_id, (SELECT cell_id FROM interactions.locations where ST_Equals("position", caller_loc::geometry)) AS cell_id, 
                                        {date_sk} AS date_sk, time_sk, 2 AS event_type, "timestamp" FROM callers
                                        RETURNING *
                                )
                                INSERT INTO interactions.subscriber_sightings (subscriber_id, cell_id, date_sk, time_sk, event_super_table_id, "timestamp") 
                                    SELECT _.subscriber_id, (SELECT cell_id FROM interactions.locations where ST_Equals("position",loc::geometry)) AS cell_id, 
                                    {date_sk} as date_sk, _.time_sk, event_id, "timestamp" from event_supertable e
                                    JOIN (
                                        SELECT id1 as subscriber_id, caller_loc AS loc, time_sk, "timestamp" FROM callers
                                        UNION ALL
                                        SELECT id2 as subscriber_id, callee_loc AS loc, time_sk, "timestamp" FROM callers
                                    ) _ USING("timestamp")
                            """,
                        )

                # 4.4 MDS
                if num_mds > 0:
                    with log_duration(f"Generating {num_mds} mds events for {date}"):
                        # Generate the distributed mds for this day
                        generatedDistributedTypes(
                            connection,
                            dist_mds,
                            date,
                            (i + 1),
                            """
                                WITH callers AS (
                                    SELECT s.id, v.value as loc, NEXTVAL('time_dimension') AS time_sk
                                    FROM subs s
                                        
                                        LEFT JOIN variations v
                                        ON v.type  = s.variant
                                        AND v.row = (select nextval('rowcount'))
                                    
                                    WHERE s.id = 1
                                )
                                INSERT INTO interactions.subscriber_sightings (subscriber_id, cell_id, date_sk, time_sk, "timestamp") (
                                    SELECT 
                                        c.id AS subscriber_id, 
                                        (SELECT cell_id FROM interactions.locations where ST_Equals("position",(c.loc->>s.point::INTEGER)::geometry)) AS cell_id, {date_sk} AS date_sk, time_sk,
                                        CONCAT('{date} ', LPAD((time_sk - 1)::TEXT, 2, '0'), ':', LPAD(NEXTVAL('minutes')::TEXT, 2, '0'), ':', LPAD(NEXTVAL('seconds')::TEXT, 2, '0'))::TIMESTAMPTZ
                                    FROM
                                        callers c,
                                        (SELECT row_number() over() AS id, nextval('pointcount') AS point FROM generate_series({from_count}, {to_count}, 2)) s
                                )
                            """,
                        )

                trans.commit()

        # Add all the ANALYZE calls for the events tables.
        deferred_sql.append(
            (
                "Analyzing the subscriber_sightings table",
                ["ANALYZE interactions.subscriber_sightings;"],
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
