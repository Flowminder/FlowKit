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
from pathlib import Path

import os
import argparse
import datetime
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
    "--n-subscribers", type=int, default=4000, help="Number of subscribers to generate."
)
parser.add_argument(
    "--n-tacs", type=int, default=4000, help="Number of phone models to generate."
)
parser.add_argument(
    "--n-sites", type=int, default=1000, help="Number of sites to generate."
)
parser.add_argument(
    "--n-cells", type=int, default=1000, help="Number of cells to generate."
)
parser.add_argument(
    "--n-calls", type=int, default=200_000, help="Number of calls to generate per day."
)
parser.add_argument(
    "--n-sms", type=int, default=200_000, help="Number of sms to generate per day."
)
parser.add_argument(
    "--n-mds", type=int, default=200_000, help="Number of mds to generate per day."
)
parser.add_argument(
    "--out-of-area-probability",
    type=float,
    default=0.05,
    help="Chance of an interaction happening outside of a subscriber's home region.",
)
parser.add_argument(
    "--relocation-probability",
    type=float,
    default=0.01,
    help="Chance of a subscriber moving from their home region each day.",
)
parser.add_argument(
    "--interactions-multiplier",
    type=int,
    default=5,
    help="Generate n*num_subscribers interaction pairs.",
)
parser.add_argument(
    "--n-days", type=int, default=7, help="Number of days of data to generate."
)
parser.add_argument(
    "--cluster", action="store_true", help="Cluster tables on msisdn index."
)
parser.add_argument(
    "--disaster-zone",
    default=False,
    type=str,
    help="Admin 2 pcode to use as disaster zone.",
)
parser.add_argument(
    "--country",
    default=False,
    type=str,
    help="ISO country code for data to be generated in.",
)
parser.add_argument(
    "--disaster-start-date",
    type=datetime.date.fromisoformat,
    help="Date to begin the disaster.",
)
parser.add_argument(
    "--disaster-end-date",
    type=datetime.date.fromisoformat,
    help="Date to end the disaster.",
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
        num_subscribers = args.n_subscribers
        num_sites = args.n_sites
        num_cells = args.n_cells
        num_calls = args.n_calls
        num_sms = args.n_sms
        num_mds = args.n_mds
        num_days = args.n_days
        num_tacs = args.n_tacs
        relocation_probability = args.relocation_probability
        out_of_area_probability = args.out_of_area_probability
        interactions_multiplier = args.interactions_multiplier
        pcode_to_knock_out = args.disaster_zone
        try:
            disaster_start_date = args.disaster_start_date
        except AttributeError:
            disaster_start_date = False
            pass  # No disaster
        try:
            disaster_end_date = args.disaster_end_date
        except AttributeError:
            disaster_end_date = datetime.date(2016, 1, 1) + datetime.timedelta(
                days=num_days
            )
        country = args.country

        engine = sqlalchemy.create_engine(
            f"postgresql://{os.getenv('POSTGRES_USER')}@/{os.getenv('POSTGRES_DB')}",
            echo=False,
            strategy="threadlocal",
            pool_size=cpu_count(),
            pool_timeout=None,
        )

        start_time = datetime.datetime.now()

        # Load geography
        available_files = list(
            Path("/docker-entrypoint-initdb.d/data/geo").glob("*[0-9].shp")
        )
        logger.info("Found shapefiles", file=available_files)
        homes_level = 0
        with engine.begin() as trans:
            trans.execute("TRUNCATE TABLE geography.geoms CASCADE;")
            for level in range(4):
                trans.execute(f"DROP TABLE IF EXISTS geography.admin{level};")
        with engine.begin() as trans:
            with log_duration(job="Load shape data."):
                for shape_file in available_files:
                    level = int(shape_file.stem[-1])
                    homes_level = max(homes_level, level)
                    with log_duration(
                        job=f"Loading admin level.", admin_level=level, file=shape_file
                    ):
                        trans.execute(
                            f"""
                        CREATE SERVER admin{level}_boundaries
                          FOREIGN DATA WRAPPER ogr_fdw
                          OPTIONS (
                            datasource '{shape_file}',
                            format 'ESRI Shapefile' );
                        """
                        )
                        if level > 0:
                            trans.execute(
                                f"""
                            CREATE FOREIGN TABLE admin{level} (
                              GID_{level} text,
                              GID_{level-1}  text,
                              NAME_{level} text,
                              geom geometry(MultiPolygon, 4326)
                            ) SERVER "admin{level}_boundaries"
                            OPTIONS (layer 'gadm36_{country}_{level}');
                            """
                            )
                            trans.execute(
                                f"""
                                INSERT INTO geography.geoms (short_name, long_name, spatial_resolution, geom, additional_metadata)
                                  SELECT GID_{level} as short_name, NAME_{level} as long_name, {level} as spatial_resolution, geom,
                                    json_build_object('parent',GID_{level-1}) as additional_metadata
                                    FROM admin{level};"""
                            )
                        else:
                            trans.execute(
                                f"""
                                CREATE FOREIGN TABLE admin{level} (
                                  NAME_{level} text,
                                  geom geometry(MultiPolygon, 4326)
                                ) SERVER "admin{level}_boundaries"
                                OPTIONS (layer 'gadm36_{country}_{level}');
                                """
                            )
                            trans.execute(
                                f"""
                                INSERT INTO geography.geoms (short_name, long_name, spatial_resolution, geom)
                                  SELECT '{country[:2]}' as short_name, NAME_{level} as long_name, {level} as spatial_resolution, geom
                                    FROM admin{level};"""
                            )

                        trans.execute(
                            f"""DROP VIEW IF EXISTS geography.admin{level};"""
                        )
                        trans.execute(
                            f"""
                        CREATE VIEW geography.admin{level} AS
                            SELECT row_number() over() as gid,
                            short_name as admin{level}pcod,
                            long_name as admin{level}name,
                            additional_metadata ->> 'parent' as parent_pcod,
                            geom from geography.geoms where spatial_resolution={level};
                        """
                        )
        homes_level = min(homes_level, 3)  # Admin3 homes at most

        # Generate some randomly distributed sites and cells
        with engine.begin() as trans:
            with log_duration(job=f"Generating {num_sites} sites."):
                trans.execute(
                    f"""CREATE TABLE tmp_sites AS 
                SELECT row_number() over() AS rid, md5(uuid_generate_v4()::text) AS id, 
                0 AS version, (date '2015-01-01' + random() * interval '1 year')::date AS date_of_first_service,
                (p).geom AS geom_point from (SELECT st_dumppoints(ST_GeneratePoints(geom, {num_sites})) AS p from geography.admin0) _;"""
                )
                trans.execute(
                    "INSERT INTO infrastructure.sites (id, version, date_of_first_service, geom_point) SELECT id, version, date_of_first_service, geom_point FROM tmp_sites;"
                )

            with log_duration(f"Generating {num_cells} cells."):
                trans.execute(
                    f"""CREATE TABLE tmp_cells as
                    SELECT row_number() over() AS rid, *, -1 AS rid_knockout FROM
                    (SELECT md5(uuid_generate_v4()::text) AS id, version, tmp_sites.id AS site_id, date_of_first_service, geom_point from tmp_sites
                    union all
                    SELECT * from 
                    (SELECT md5(uuid_generate_v4()::text) AS id, version, tmp_sites.id AS site_id, date_of_first_service, geom_point from
                    (
                      SELECT floor(random() * {num_sites} + 1)::integer AS id
                      from generate_series(1, {int(num_cells * 1.1)}) -- Preserve duplicates
                    ) rands
                    inner JOIN tmp_sites
                    ON rands.id=tmp_sites.rid
                    limit {num_cells - num_sites}) _) _
                    ;
                    CREATE INDEX ON tmp_cells (rid);
                    CREATE INDEX ON tmp_cells USING gist(geography(geom_point));
                    CREATE INDEX ON tmp_cells USING gist(geom_point);
                    """
                )
                trans.execute(
                    "INSERT INTO infrastructure.cells (id, version, site_id, date_of_first_service, geom_point) SELECT id, version, site_id, date_of_first_service, geom_point FROM tmp_cells;"
                )

            with log_duration(f"Generating {num_tacs} tacs."):
                trans.execute(
                    f"""CREATE TABLE tacs as
                (SELECT (row_number() over())::numeric(8, 0) AS tac, 
                (ARRAY['Nokia', 'Huawei', 'Apple', 'Samsung', 'Sony', 'LG', 'Google', 'Xiaomi', 'ZTE'])[floor((random()*9 + 1))::int] AS brand, 
                uuid_generate_v4()::text AS model, 
                (ARRAY['Smart', 'Feature', 'Basic'])[floor((random()*3 + 1))::int] AS  hnd_type 
                FROM generate_series(1, {num_tacs}));"""
                )
                trans.execute(
                    "INSERT INTO infrastructure.tacs (id, brand, model, hnd_type) SELECT tac AS id, brand, model, hnd_type FROM tacs;"
                )

            with log_duration(f"Generating {num_subscribers} subscribers."):
                trans.execute(
                    f"""
                CREATE TABLE subs as
                (SELECT row_number() over() AS id, md5(uuid_generate_v4()::text) AS msisdn, md5(uuid_generate_v4()::text) AS imei, 
                md5(uuid_generate_v4()::text) AS imsi, floor(random() * {num_tacs} + 1)::numeric(8, 0) AS tac 
                FROM generate_series(1, {num_subscribers}));
                CREATE INDEX on subs (id);
                ANALYZE subs;
                """
                )

        with log_duration("Generating disaster."):
            with engine.begin() as trans:
                trans.execute(
                    f"""CREATE TABLE available_cells AS 
                        SELECT '2016-01-01'::date + rid*interval '1 day' AS day,
                        CASE WHEN ('2016-01-01'::date + rid*interval '1 day' BETWEEN '{disaster_start_date}'::date AND '{disaster_end_date}'::date) THEN
                            (array(SELECT tmp_cells.id FROM tmp_cells INNER JOIN (SELECT * FROM geography.admin2 WHERE admin2pcod != '{pcode_to_knock_out}') _ ON ST_Within(geom_point, geom)))
                        ELSE
                            (array(SELECT tmp_cells.id FROM tmp_cells))
                        END AS cells
                        FROM generate_series(0, {num_days}) AS t(rid);"""
                )
            with engine.begin() as trans:
                trans.execute("CREATE INDEX ON available_cells (day);")
                trans.execute("ANALYZE available_cells;")

        with log_duration("Assigning subscriber home regions."):
            with engine.begin() as trans:
                trans.execute(
                    f"""CREATE TABLE homes AS (
                       SELECT h.*, cells FROM
                       (SELECT '2016-01-01'::date AS home_date, id, admin{homes_level}pcod AS home_id FROM (
                       SELECT *, floor(random() * (SELECT count(distinct admin{homes_level}pcod) from geography.admin{homes_level} RIGHT JOIN tmp_cells on ST_Within(geom_point, geom)) + 1)::integer AS admin_id FROM subs
                       ) subscribers
                       LEFT JOIN
                       (SELECT row_number() over() AS admin_id, admin{homes_level}pcod FROM 
                       geography.admin{homes_level}
                       RIGHT JOIN
                       tmp_cells ON ST_Within(geom_point, geom)
                       ) geo
                       ON geo.admin_id=subscribers.admin_id) h
                       LEFT JOIN 
                       (SELECT admin{homes_level}pcod, array_agg(id) AS cells FROM tmp_cells LEFT JOIN geography.admin{homes_level} ON ST_Within(geom_point, geom) GROUP BY admin{homes_level}pcod) c
                       ON admin{homes_level}pcod=home_id
                       )
                       """
                )

            for date in (
                datetime.date(2016, 1, 2) + datetime.timedelta(days=i)
                for i in range(num_days)
            ):
                with engine.begin() as trans:
                    if (
                        pcode_to_knock_out
                        and disaster_start_date
                        and disaster_start_date <= date <= disaster_end_date
                    ):
                        trans.execute(
                            f"""INSERT INTO homes
                                                SELECT h.*, cells FROM
                                                (SELECT '{date.strftime(
                                "%Y-%m-%d")}'::date AS home_date, id, CASE WHEN (random() > {1 - relocation_probability} OR home_id = ANY((array(SELECT admin{homes_level}.admin{homes_level}pcod FROM geography.admin{homes_level} WHERE parent_pcod = '{pcode_to_knock_out}')))) THEN admin{homes_level}pcod ELSE home_id END AS home_id FROM (
                                                SELECT *, floor(random() * (SELECT count(distinct admin{homes_level}pcod) from geography.admin{homes_level} RIGHT JOIN tmp_cells on ST_Within(geom_point, geom) WHERE parent_pcod!='{pcode_to_knock_out}') + 1)::integer AS admin_id FROM homes
                                                WHERE home_date='{(date - datetime.timedelta(days=1)).strftime(
                                "%Y-%m-%d")}'::date
                                                ) subscribers
                                                LEFT JOIN
                                                (SELECT row_number() over() AS admin_id, admin{homes_level}pcod FROM 
                                                geography.admin{homes_level}
                                                RIGHT JOIN
                                                tmp_cells ON ST_Within(geom_point, geom)
                                                WHERE parent_pcod!='{pcode_to_knock_out}'
                                                ) geo
                                                ON geo.admin_id=subscribers.admin_id) h
                                                LEFT JOIN 
                                                        (SELECT admin{homes_level}pcod, array_agg(id) AS cells FROM tmp_cells LEFT JOIN geography.admin{homes_level} ON ST_Within(geom_point, geom) GROUP BY admin{homes_level}pcod) c
                                                        ON admin{homes_level}pcod=home_id
                                                """
                        )
                    else:
                        trans.execute(
                            f"""INSERT INTO homes
                                SELECT h.*, cells FROM
                                (SELECT '{date.strftime(
                                "%Y-%m-%d")}'::date AS home_date, id, CASE WHEN (random() > {1 - relocation_probability}) THEN admin{homes_level}pcod ELSE home_id END AS home_id FROM (
                                SELECT *, floor(random() * (SELECT count(distinct admin{homes_level}pcod) from geography.admin{homes_level} RIGHT JOIN tmp_cells on ST_Within(geom_point, geom)) + 1)::integer AS admin_id FROM homes
                                WHERE home_date='{(date - datetime.timedelta(days=1)).strftime("%Y-%m-%d")}'::date
                                ) subscribers
                                LEFT JOIN
                                (SELECT row_number() over() AS admin_id, admin{homes_level}pcod FROM 
                                geography.admin{homes_level}
                                RIGHT JOIN
                                tmp_cells ON ST_Within(geom_point, geom)
                                ) geo
                                ON geo.admin_id=subscribers.admin_id) h
                                LEFT JOIN 
                                        (SELECT admin{homes_level}pcod, array_agg(id) AS cells FROM tmp_cells LEFT JOIN geography.admin{homes_level} ON ST_Within(geom_point, geom) GROUP BY admin{homes_level}pcod) c
                                        ON admin{homes_level}pcod=home_id
                                """
                        )
            with engine.begin() as trans:
                trans.execute("ANALYZE homes;")
                trans.execute("CREATE INDEX ON homes (id);")
                trans.execute("CREATE INDEX ON homes (home_date);")
                trans.execute("CREATE INDEX ON homes (home_date, id);")

        with log_duration(
            f"Generating {num_subscribers * interactions_multiplier} interaction pairs."
        ):
            with engine.begin() as trans:
                trans.execute(
                    f"""CREATE TABLE interactions AS SELECT 
                        row_number() over() AS rid, callee_id, caller_id, caller.msisdn AS caller_msisdn, 
                            caller.tac AS caller_tac, caller.imsi AS caller_imsi, caller.imei AS caller_imei, 
                            callee.msisdn AS callee_msisdn, callee.tac AS callee_tac, 
                            callee.imsi AS callee_imsi, callee.imei AS callee_imei FROM
                        (SELECT 
                        floor(random() * {num_subscribers} + 1)::integer AS caller_id, 
                        floor(random() * {num_subscribers} + 1)::integer AS callee_id FROM 
                        generate_series(1, {num_subscribers * interactions_multiplier})) AS pairs
                        LEFT JOIN subs AS caller ON pairs.caller_id = caller.id
                        LEFT JOIN subs AS callee ON pairs.callee_id = callee.id
                    """
                )
                trans.execute("CREATE INDEX ON interactions (rid);")
                trans.execute("ANALYZE interactions;")

        event_creation_sql = []
        sql = []
        for date in (
            datetime.date(2016, 1, 1) + datetime.timedelta(days=i)
            for i in range(num_days)
        ):
            table = date.strftime("%Y%m%d")
            end_date = (date + datetime.timedelta(days=1)).strftime("%Y-%m-%d")

            if num_calls > 0:
                event_creation_sql.append(
                    (
                        f"Generating {num_calls} call events for {date}",
                        f"""
                CREATE TABLE call_evts_{table} AS
                SELECT ('{table}'::TIMESTAMPTZ + random() * interval '1 day') AS start_time,
                round(random()*2600)::numeric AS duration,
                uuid_generate_v4()::text AS id, interactions.*,
                CASE WHEN (random() > {1 - out_of_area_probability}) THEN
                    available_cells.cells[floor(random()*array_length(available_cells.cells, 1) + 1)]
                ELSE
                     caller_homes.cells[floor(random()*array_length(caller_homes.cells, 1) + 1)]
                END AS caller_cell,
                CASE WHEN (random() > {1 - out_of_area_probability}) THEN
                    available_cells.cells[floor(random()*array_length(available_cells.cells, 1) + 1)]
                ELSE
                     callee_homes.cells[floor(random()*array_length(callee_homes.cells, 1) + 1)]
                END AS callee_cell
                FROM 
                (SELECT floor(random()*{num_subscribers * interactions_multiplier} + 1)::integer AS rid FROM
                generate_series(1, {num_calls})) _
                LEFT JOIN
                interactions
                USING (rid)
                LEFT JOIN available_cells
                ON day='{table}'::date
                LEFT JOIN homes AS caller_homes
                ON caller_homes.home_date='{table}'::date and caller_homes.id=interactions.caller_id
                LEFT JOIN homes AS callee_homes
                ON callee_homes.home_date='{table}'::date and callee_homes.id=interactions.callee_id;
    
                CREATE TABLE events.calls_{table} AS 
                SELECT id, true AS outgoing, start_time AS datetime, duration, NULL::TEXT AS network,
                caller_msisdn AS msisdn, callee_msisdn AS msisdn_counterpart, caller_cell AS location_id,
                caller_imsi AS imsi, caller_imei AS imei, caller_tac AS tac, NULL::NUMERIC AS operator_code,
                NULL::NUMERIC AS country_code
                FROM call_evts_{table}
                UNION ALL 
                SELECT id, false AS outgoing, start_time AS datetime, duration, NULL::TEXT AS network,
                callee_msisdn AS msisdn, caller_msisdn AS msisdn_counterpart, callee_cell AS location_id,
                callee_imsi AS imsi, callee_imei AS imei, callee_tac AS tac, NULL::NUMERIC AS operator_code,
                NULL::NUMERIC AS country_code
                FROM call_evts_{table};
                ALTER TABLE events.calls_{table} ADD CONSTRAINT calls_{table}_dt CHECK ( datetime >= '{table}'::TIMESTAMPTZ AND datetime < '{end_date}'::TIMESTAMPTZ);
                ALTER TABLE events.calls_{table} ALTER msisdn SET NOT NULL;
                ALTER TABLE events.calls_{table} ALTER datetime SET NOT NULL;
                """,
                    )
                )

            if num_sms > 0:
                event_creation_sql.append(
                    (
                        f"Generating {num_sms} sms events for {date}",
                        f"""
                CREATE TABLE sms_evts_{table} AS
                SELECT ('{table}'::TIMESTAMPTZ + random() * interval '1 day') AS start_time,
                uuid_generate_v4()::text AS id, interactions.*,
                CASE WHEN (random() > {1 - out_of_area_probability}) THEN
                    available_cells.cells[floor(random()*array_length(available_cells.cells, 1) + 1)]
                ELSE
                     caller_homes.cells[floor(random()*array_length(caller_homes.cells, 1) + 1)]
                END AS caller_cell,
                CASE WHEN (random() > {1 - out_of_area_probability}) THEN
                    available_cells.cells[floor(random()*array_length(available_cells.cells, 1) + 1)]
                ELSE
                     callee_homes.cells[floor(random()*array_length(callee_homes.cells, 1) + 1)]
                END AS callee_cell
                FROM 
                (SELECT floor(random()*{num_subscribers * interactions_multiplier} + 1)::integer AS rid FROM
                generate_series(1, {num_sms})) _
                LEFT JOIN
                interactions
                USING (rid)
                LEFT JOIN available_cells
                ON day='{table}'::date
                LEFT JOIN homes AS caller_homes
                ON caller_homes.home_date='{table}'::date and caller_homes.id=interactions.caller_id
                LEFT JOIN homes AS callee_homes
                ON callee_homes.home_date='{table}'::date and callee_homes.id=interactions.callee_id;
    
                CREATE TABLE events.sms_{table} AS 
                SELECT id, true AS outgoing, start_time AS datetime, NULL::TEXT AS network,
                caller_msisdn AS msisdn, callee_msisdn AS msisdn_counterpart, caller_cell AS location_id,
                caller_imsi AS imsi, caller_imei AS imei, caller_tac AS tac, NULL::NUMERIC AS operator_code,
                NULL::NUMERIC AS country_code
                FROM sms_evts_{table}
                UNION ALL 
                SELECT id, false AS outgoing, start_time AS datetime, NULL::TEXT AS network,
                callee_msisdn AS msisdn, caller_msisdn AS msisdn_counterpart, callee_cell AS location_id,
                callee_imsi AS imsi, callee_imei AS imei, callee_tac AS tac, NULL::NUMERIC AS operator_code,
                NULL::NUMERIC AS country_code
                FROM sms_evts_{table};
                ALTER TABLE events.sms_{table} ADD CONSTRAINT sms_{table}_dt CHECK ( datetime >= '{table}'::TIMESTAMPTZ AND datetime < '{end_date}'::TIMESTAMPTZ);
                ALTER TABLE events.sms_{table} ALTER msisdn SET NOT NULL;
                ALTER TABLE events.sms_{table} ALTER datetime SET NOT NULL;
                """,
                    )
                )

            if num_mds > 0:
                event_creation_sql.append(
                    (
                        f"Generating {num_mds} mds events for {date}",
                        f"""
                CREATE TABLE events.mds_{table} AS
                SELECT uuid_generate_v4()::text AS id, ('{table}'::TIMESTAMPTZ + random() * interval '1 day') AS datetime, 
                round(random() * 260)::numeric AS duration, volume_upload + volume_download AS volume_total, volume_upload,
                volume_download, msisdn, cell AS location_id, imsi, imei, tac, 
                NULL::NUMERIC AS operator_code, NULL::NUMERIC AS country_code
                FROM
                (SELECT
                subs.msisdn, subs.imsi, subs.imei, subs.tac,
                round(random() * 100000)::numeric AS volume_upload, 
                round(random() * 100000)::numeric AS volume_download,
                CASE WHEN (random() > {1 - out_of_area_probability}) THEN
                    available_cells.cells[floor(random()*array_length(available_cells.cells, 1) + 1)]
                ELSE
                     caller_homes.cells[floor(random()*array_length(caller_homes.cells, 1) + 1)]
                END AS cell
                FROM (SELECT floor(random()*{num_subscribers} + 1)::integer AS id FROM
                generate_series(1, {num_mds})) _
                LEFT JOIN
                subs
                USING (id)
                LEFT JOIN available_cells
                ON day='{table}'::date
                LEFT JOIN homes AS caller_homes
                ON caller_homes.home_date='{table}'::date and caller_homes.id=subs.id) _;
                ALTER TABLE events.mds_{table} ADD CONSTRAINT mds_{table}_dt CHECK ( datetime >= '{table}'::TIMESTAMPTZ AND datetime < '{end_date}'::TIMESTAMPTZ);
                ALTER TABLE events.mds_{table} ALTER msisdn SET NOT NULL;
                ALTER TABLE events.mds_{table} ALTER datetime SET NOT NULL;
                """,
                    )
                )

        post_sql = []
        attach_sql = []
        post_attach_sql = []
        cleanup_sql = []
        for sub in ("calls", "sms", "mds"):
            if getattr(args, f"n_{sub}") > 0:
                for date in (
                    datetime.date(2016, 1, 1) + datetime.timedelta(days=i)
                    for i in range(num_days)
                ):
                    table = date.strftime("%Y%m%d")
                    cleanup_sql.append(
                        (
                            f"Dropping {sub}_evts_{table}",
                            f"DROP TABLE IF EXISTS {sub}_evts_{table};",
                        )
                    )
                    attach_sql.append(
                        (
                            f"Attaching events.{sub}_{table}",
                            f"ALTER TABLE events.{sub}_{table} INHERIT events.{sub};",
                        )
                    )
                    if args.cluster:
                        post_sql.append(
                            (
                                f"Clustering events.{sub}_{table}.",
                                f"CLUSTER events.{sub}_{table} USING {sub}_{table}_msisdn_idx;",
                            )
                        )
                    post_sql.append(
                        (
                            f"Analyzing events.{sub}_{table}",
                            f"ANALYZE events.{sub}_{table};",
                        )
                    )
                    post_sql.append(
                        (
                            f"Mark events.{sub}_{table} as available in etl.",
                            f"""
                            INSERT INTO etl.etl_records (cdr_type, cdr_date, state, timestamp) VALUES
                            ('{sub}', '{date}'::DATE, 'ingested', NOW());""",
                        )
                    )
                # Mark tables as available for flowmachine
                post_sql.append(
                    (
                        f"Updating availability for {sub}",
                        f"""
                INSERT INTO available_tables (table_name, has_locations, has_subscribers, has_counterparts) VALUES ('{sub}', true, true, true)
                ON conflict (table_name)
                DO UPDATE SET has_locations=EXCLUDED.has_locations, has_subscribers=EXCLUDED.has_subscribers, has_counterparts=EXCLUDED.has_counterparts;
                """,
                    )
                )
            post_attach_sql.append((f"Analyzing {sub}", f"ANALYZE events.{sub};"))

        # Remove the intermediary data tables
        for tbl in (
            "tmp_cells",
            "tmp_sites",
            "subs",
            "tacs",
            "available_cells",
            "homes",
            "interactions",
        ):
            cleanup_sql.append((f"Dropping {tbl}", f"DROP TABLE {tbl};"))

        def do_exec(args):
            msg, sql = args
            with log_duration(msg):
                with engine.begin() as trans:
                    res = trans.execute(sql)
                    try:
                        logger.info(f"SQL result", job=msg, result=res.fetchall())
                    except ResourceClosedError:
                        pass  # Nothing to do here

        with ThreadPoolExecutor(cpu_count()) as tp:
            list(tp.map(do_exec, event_creation_sql))
            list(tp.map(do_exec, post_sql))
        for s in attach_sql + cleanup_sql:
            do_exec(s)
        with ThreadPoolExecutor(cpu_count()) as tp:
            list(tp.map(do_exec, post_attach_sql))
