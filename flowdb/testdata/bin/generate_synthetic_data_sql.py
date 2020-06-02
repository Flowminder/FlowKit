# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# !/usr/bin/env python

"""
Small script for generating arbitrary volumes of CDR call data inside the flowdb
container.

Produces sites, cells, tacs, call, sms and mds data.

Optionally simulates a 'disaster' where all subscribers must leave a designated region
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
                            f"DROP SERVER IF EXISTS admin{level}_boundaries CASCADE;"
                        )
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

        with engine.begin() as trans:
            with log_duration(job="Generating population distribution."):
                trans.execute(
                    f"""
                CREATE TABLE population_density AS
                SELECT pcod as short_name, p_pop, cdf, numrange(cdf::numeric, lead(cdf::numeric) over (order by pcod), '(]') as cdf_range FROM
                    (SELECT pcod, p_pop, coalesce(sum(p_pop) OVER (ORDER BY pcod ROWS UNBOUNDED PRECEDING EXCLUDE CURRENT ROW), 0) as cdf 
                    FROM
                        (SELECT pcod, pop / sum(pop) OVER () as p_pop 
                        FROM
                            (SELECT G.admin{homes_level}pcod as pcod, sum((st_summarystats(st_clip(R.rast, ARRAY[1], G.geom::geometry))).sum) as pop 
                            FROM population AS R, geography.admin{homes_level} AS G WHERE st_intersects(g.geom::geometry, R.rast) 
                                GROUP BY admin{homes_level}pcod) _) _) _;
                """
                )
        # Generate some randomly distributed sites and cells
        with engine.begin() as trans:
            trans.execute("DROP TABLE IF EXISTS tmp_sites;")
            trans.execute("TRUNCATE TABLE infrastructure.sites CASCADE;")
            with log_duration(job=f"Generating {num_sites} sites."):
                trans.execute(
                    f"""
                CREATE TABLE tmp_sites AS 
                SELECT row_number() over() AS rid, md5(uuid_generate_v4()::text) AS id, 
                0 AS version, (date '2015-01-01' + random() * interval '1 year')::date AS date_of_first_service,
                (p).geom AS geom_point FROM (SELECT st_dumppoints(ST_GeneratePoints(geom, n_sites::int)) AS p FROM
                    (
                    SELECT geom, n_sites FROM (
                        SELECT short_name, count(*) as n_sites 
                        FROM population_density 
                        RIGHT JOIN (
                            SELECT random()::numeric as p 
                            FROM generate_series(1, {num_sites})
                        ) _ 
                        ON p <@ cdf_range 
                        GROUP BY short_name
                    ) _
                    LEFT JOIN 
                    geography.geoms USING (short_name)
                ) as site_counts) _;"""
                )
                trans.execute(
                    "INSERT INTO infrastructure.sites (id, version, date_of_first_service, geom_point) SELECT id, version, date_of_first_service, geom_point FROM tmp_sites;"
                )

            with log_duration(f"Generating {num_cells} cells."):
                trans.execute("DROP TABLE IF EXISTS tmp_cells;")
                trans.execute("TRUNCATE TABLE infrastructure.cells CASCADE;")
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
                trans.execute("DROP TABLE IF EXISTS tacs;")
                trans.execute("TRUNCATE TABLE infrastructure.tacs CASCADE;")
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
                trans.execute("DROP TABLE IF EXISTS subs;")
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
                trans.execute("DROP TABLE IF EXISTS bad_cells;")
                trans.execute(
                    f"CREATE TABLE bad_cells AS SELECT tmp_cells.id FROM tmp_cells INNER JOIN (SELECT * FROM geography.geoms WHERE short_name != '{pcode_to_knock_out}') _ ON ST_Within(geom_point, geom)"
                )
                trans.execute("DROP TABLE IF EXISTS available_cells;")
                trans.execute(
                    f"""CREATE TABLE available_cells AS 
                        SELECT '2016-01-01'::date + rid*interval '1 day' AS day,
                        CASE WHEN ('2016-01-01'::date + rid*interval '1 day' BETWEEN '{disaster_start_date}'::date AND '{disaster_end_date}'::date) THEN
                            (array(SELECT id FROM bad_cells))
                        ELSE
                            (array(SELECT tmp_cells.id FROM tmp_cells))
                        END AS cells
                        FROM generate_series(0, {num_days}) AS t(rid);"""
                )
            with engine.begin() as trans:
                trans.execute("CREATE INDEX ON available_cells (day);")
                trans.execute("ANALYZE available_cells;")

        with log_duration("Assigning subscriber home regions."):
            with log_duration("Initial homes."):
                with engine.begin() as trans:
                    trans.execute("DROP TABLE IF EXISTS homes;")
                    trans.execute("DROP TABLE IF EXISTS tmp_homes;")
                    trans.execute(
                        f"""
                    CREATE TABLE tmp_homes AS
                        SELECT s.id, 
                            moved_in::date, 
                            home_cell, 
                            cells 
                        FROM
                            (SELECT id, '2016-01-01' as moved_in, floor(1+random()*{num_cells}) as home_cell_id from subs) s
                            LEFT JOIN 
                                (SELECT tmp_cells.rid as home_cell_id, tmp_cells.id as home_cell, array_agg(tmp3.id) as cells
                                    FROM tmp_cells  
                                     LEFT JOIN tmp_cells AS tmp3 ON 
                                        st_dwithin(tmp_cells.geom_point::geography, tmp3.geom_point::geography, 3000)
                                     GROUP BY tmp_cells.rid, tmp_cells.id) AS cells
                            USING (home_cell_id);
                    """
                    )

            for date in (
                datetime.date(2016, 1, 2) + datetime.timedelta(days=i)
                for i in range(num_days)
            ):
                if date == disaster_start_date:
                    with log_duration(
                        "Assigning subscriber home regions + disaster.", day=date
                    ):
                        try:
                            with engine.begin() as trans:
                                trans.execute(
                                    f"""
                                    WITH subs_to_move_randomly AS (
                                        SELECT id, '{date.strftime("%Y-%m-%d")}' as moved_in, 
                                        random_pick((select cells from available_cells where day='{date.strftime("%Y-%m-%d")}')::text[]) as home_cell 
                                        FROM subs ORDER BY random() LIMIT floor(random_poisson({num_subscribers * relocation_probability}))),
                                    subs_to_rehome AS (
                                        SELECT s.id, '{date.strftime("%Y-%m-%d")}' as moved_in, 
                                        random_pick((select cells from available_cells where day='{date.strftime("%Y-%m-%d")}')::text[]) as home_cell
                                        FROM (SELECT first_value(id) over (partition by id order by moved_in desc) as id, first_value(home_cell) over (partition by id order by moved_in desc) as home_cell from tmp_homes) s
                                        LEFT JOIN bad_cells ON home_cell=bad_cells.id
    
                                    ),
                                    subs_to_move AS (select * from subs_to_move_randomly union select * from subs_to_rehome)
                                    INSERT INTO tmp_homes
                                    SELECT id, 
                                    moved_in::date, 
                                    home_cell, 
                                    cells
                                    FROM
                                    subs_to_move
                                    LEFT JOIN 
                                        (SELECT tmp_cells.id as home_cell, array_agg(tmp3.id) as cells 
                                            FROM tmp_cells 
                                             LEFT JOIN 
                                             tmp_cells as tmp3 ON st_dwithin(tmp_cells.geom_point::geography, tmp3.geom_point::geography, 3000)
                                             GROUP BY tmp_cells.id) AS cells
                                    USING (home_cell);
                                """
                                )
                        except Exception as exc:
                            print(exc)
                            exit(0)
                else:
                    with log_duration("Assigning subscriber home regions.", day=date):
                        with engine.begin() as trans:
                            trans.execute(
                                f"""
                                WITH subs_to_move_randomly AS (
                                    SELECT id, '{date.strftime("%Y-%m-%d")}' as moved_in, 
                                    random_pick((select cells from available_cells where day='{date.strftime("%Y-%m-%d")}')::text[]) as home_cell 
                                    FROM subs ORDER BY random() LIMIT floor(random_poisson({num_subscribers*relocation_probability})))
                                INSERT INTO tmp_homes
                                SELECT id, 
                                moved_in::date, 
                                home_cell, 
                                cells
                                FROM
                                subs_to_move_randomly
                                LEFT JOIN 
                                    (SELECT tmp_cells.id as home_cell, array_agg(tmp3.id) as cells 
                                        FROM tmp_cells 
                                         LEFT JOIN 
                                         tmp_cells as tmp3 ON st_dwithin(tmp_cells.geom_point::geography, tmp3.geom_point::geography, 3000)
                                         GROUP BY tmp_cells.id) AS cells
                                USING (home_cell);
                            """
                            )
            with log_duration("Partitioning homes."):
                with engine.begin() as trans:
                    trans.execute(
                        """CREATE TABLE HOMES AS
                    SELECT id, daterange(moved_in, lead(moved_in) over (partition by id order by moved_in asc)) as home_date, cells
                    FROM tmp_homes;
                    """
                    )
            with log_duration("Analyzing and indexing subscriber home regions."):
                with engine.begin() as trans:
                    trans.execute("ANALYZE homes;")
                    trans.execute("CREATE INDEX ON homes (id);")
                    trans.execute("CREATE INDEX ON homes (home_date);")
                    trans.execute("CREATE INDEX ON homes (home_date, id);")

        with log_duration(
            f"Generating {num_subscribers * interactions_multiplier} interaction pairs."
        ):
            with engine.begin() as trans:
                trans.execute("DROP TABLE IF EXISTS interactions;")
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
                    random_pick(available_cells.cells)
                ELSE
                     random_pick(caller_homes.cells)
                END AS caller_cell,
                CASE WHEN (random() > {1 - out_of_area_probability}) THEN
                    random_pick(available_cells.cells)
                ELSE
                     random_pick(callee_homes.cells)
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
                ON caller_homes.home_date@>'{table}'::date and caller_homes.id=interactions.caller_id
                LEFT JOIN homes AS callee_homes
                ON callee_homes.home_date@>'{table}'::date and callee_homes.id=interactions.callee_id;
    
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
                    random_pick(available_cells.cells)
                ELSE
                     random_pick(caller_homes.cells)
                END AS caller_cell,
                CASE WHEN (random() > {1 - out_of_area_probability}) THEN
                    random_pick(available_cells.cells)
                ELSE
                     random_pick(callee_homes.cells)
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
                ON caller_homes.home_date@>'{table}'::date and caller_homes.id=interactions.caller_id
                LEFT JOIN homes AS callee_homes
                ON callee_homes.home_date@>'{table}'::date and callee_homes.id=interactions.callee_id;
    
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
                    random_pick(available_cells.cells)
                ELSE
                     random_pick(caller_homes.cells)
                END AS cell
                FROM (SELECT floor(random()*{num_subscribers} + 1)::integer AS id FROM
                generate_series(1, {num_mds})) _
                LEFT JOIN
                subs
                USING (id)
                LEFT JOIN available_cells
                ON day='{table}'::date
                LEFT JOIN homes AS caller_homes
                ON caller_homes.home_date@>'{table}'::date and caller_homes.id=subs.id) _;
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
                            f"Indexing events.{sub}_{table}",
                            f"""CREATE INDEX ON events.{sub}_{table}(msisdn);
                                CREATE INDEX ON events.{sub}_{table}(datetime);
                                CREATE INDEX ON events.{sub}_{table}(location_id);
                            """,
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
