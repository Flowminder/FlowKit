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
from hashlib import md5
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
parser = argparse.ArgumentParser(description="Flowminder Synthetic CDR Generator\n")

parser.add_argument(
    "--n-sites", type=int, default=1000, help="Number of sites to generate."
)
parser.add_argument(
    "--n-cells", type=int, default=1000, help="Number of cells to generate."
)


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
    with log_duration("Generating synthetic data..", **vars(args)):
        # Limit num_sites to 10000 due to geom.dat.
        num_sites = min(10000, args.n_sites)
        num_cells = args.n_cells

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

        # Generate some randomly distributed sites and cells
        with engine.begin() as trans:
            with log_duration(job=f"Generating {num_sites} sites."):
                with open(f"{dir}/../synthetic_data/data/geom.dat", "r") as f:
                    # First truncate the table
                    trans.execute("TRUNCATE infrastructure.sites;")

                    for x in range(start_id, num_sites + start_id):
                        hash = md5(int(x).to_bytes(8, "big", signed=True)).hexdigest()
                        geom_point = f.readline().strip()

                        trans.execute(
                            f"""
                                INSERT INTO infrastructure.sites (id, version, date_of_first_service, geom_point) 
                                VALUES ('{hash}', 0, (date '2015-01-01' + random() * interval '1 year')::date, '{geom_point}');
                            """
                        )

                    f.close()

            with log_duration(f"Generating {num_cells} cells."):
                for x in range(start_id, num_sites + start_id):
                    sitehash = md5(int(x).to_bytes(8, "big", signed=True)).hexdigest()

                    for y in range(start_id, num_cells + start_id):
                        cellhash = md5(
                            int(x).to_bytes(8, "big", signed=True)
                        ).hexdigest()

                        trans.execute(
                            f"""
                                INSERT INTO infrastructure.cells (id, version, site_id, date_of_first_service, geom_point) 
                                VALUES ('{hash}', 0, (date '2015-01-01' + random() * interval '1 year')::date, null);
                            """
                        )
                # trans.execute(
                #     f"""CREATE TABLE tmp_cells as
                #     SELECT row_number() over() AS rid, *, -1 AS rid_knockout FROM
                #     (SELECT md5(uuid_generate_v4()::text) AS id, version, tmp_sites.id AS site_id, date_of_first_service, geom_point from tmp_sites
                #     union all
                #     SELECT * from
                #     (SELECT md5(uuid_generate_v4()::text) AS id, version, tmp_sites.id AS site_id, date_of_first_service, geom_point from
                #     (
                #       SELECT floor(random() * {num_sites} + 1)::integer AS id
                #       from generate_series(1, {int(num_cells * 1.1)}) -- Preserve duplicates
                #     ) rands
                #     inner JOIN tmp_sites
                #     ON rands.id=tmp_sites.rid
                #     limit {num_cells - num_sites}) _) _
                #     ;
                #     CREATE INDEX ON tmp_cells (rid);
                #     """
                # )
                # trans.execute(
                #     "INSERT INTO infrastructure.cells (id, version, site_id, date_of_first_service, geom_point) SELECT id, version, site_id, date_of_first_service, geom_point FROM tmp_cells;"
                # )
