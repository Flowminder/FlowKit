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
import sys
import argparse
import datetime
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
    "--n-cells", type=int, default=10, help="Number of cells to generate per site."
)
parser.add_argument(
    "--n-tacs", type=int, default=2000, help="Number of phone models to generate."
)
parser.add_argument(
    "--n-subscribers", type=int, default=4000, help="Number of subscribers to generate."
)
parser.add_argument(
    "--n-calls", type=int, default=200_000, help="Number of calls to generate per day."
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
        num_calls = args.n_calls

        engine = sqlalchemy.create_engine(
            f"postgresql://{os.getenv('POSTGRES_USER')}@/{os.getenv('POSTGRES_DB')}",
            echo=False,
            strategy="threadlocal",
            pool_size=cpu_count(),
            pool_timeout=None,
        )

        start_time = datetime.datetime.now()
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

                # Get a random distribution for the brand types across the data set
                distribution = generateNormalDistribution(num_tacs, 3)
                y = 0
                type = cycle(types)
                for x in range(start_id, num_tacs + start_id):
                    id = x + 1000
                    idx = floor(distribution[y])
                    hash = generate_hash(id)
                    y += 1

                    trans.execute(
                        f"""
                            INSERT INTO infrastructure.tacs (id, brand, model, hnd_type) VALUES ({id}, '{brands[idx]}', '{hash}', '{next(type)}');
                        """
                    )

            # 3. Temporary subscribers
            with log_duration(f"Generating {num_subscribers} subscribers."):
                mu = 10  # 10 subscribers per tac
                subscribers = generateNormalDistribution(num_subscribers)
                trans.execute(
                    f"""
                        CREATE TABLE IF NOT EXISTS subs(
                            id NUMERIC PRIMARY KEY,
                            msisdn TEXT,
                            imei TEXT,
                            imsi TEXT,
                            tac TEXT
                        );
                    """
                )

                # TODO - distribute the TAC data across the subscribers
