# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# !/usr/bin/env python

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
import argparse
import datetime
from concurrent.futures.thread import ThreadPoolExecutor
from multiprocessing import cpu_count

import sqlalchemy as sqlalchemy
from sqlalchemy.exc import ResourceClosedError

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
    "--n-days", type=int, default=7, help="Number of days of data to generate."
)
parser.add_argument(
    "--cluster", action="store_true", help="Cluster tables on msisdn index."
)
parser.add_argument(
    "--disaster-zone",
    default=False,
    type=str,
    help="Admin 2 pcod to use as disaster zone.",
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
parser.add_argument(
    "--output-root-dir",
    type=str,
    default="",
    help="Root directory under which output .sql is stored (in appropriate subfolders).",
)

if __name__ == "__main__":
    print("Generating synthetic data..")
    args = parser.parse_args()
    print(args)
    num_subscribers = args.n_subscribers
    num_sites = args.n_sites
    num_cells = args.n_cells
    num_calls = args.n_calls
    num_sms = args.n_sms
    num_mds = args.n_mds
    num_days = args.n_days
    num_tacs = args.n_tacs
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

    engine = sqlalchemy.create_engine(
        f"postgresql://{os.getenv('POSTGRES_USER')}@/{os.getenv('POSTGRES_DB')}",
        echo=False,
        strategy="threadlocal",
        pool_size=cpu_count(),
        pool_timeout=None,
    )

    start_time = datetime.datetime.now()

    with engine.begin() as trans:
        print(f"Generating {num_sites} sites.")
        start = datetime.datetime.now()
        trans.execute(
            f"""CREATE TEMPORARY TABLE tmp_sites as 
        select row_number() over() as rid, md5(uuid_generate_v4()::text) as id, 
        0 as version, (date '2015-01-01' + random() * interval '1 year')::date as date_of_first_service,
        (p).geom as geom_point from (select st_dumppoints(ST_GeneratePoints(geom, {num_sites})) as p from geography.admin0) _;"""
        )
        trans.execute(
            "INSERT INTO infrastructure.sites (id, version, date_of_first_service, geom_point) SELECT id, version, date_of_first_service, geom_point FROM tmp_sites;"
        )
        print(f"Done. Runtime: {datetime.datetime.now() - start}")
        print(f"Generating {num_cells} cells.")
        start = datetime.datetime.now()
        trans.execute(
            f"""CREATE TABLE tmp_cells as
            SELECT row_number() over() as rid, *, -1 as rid_knockout FROM
            (select md5(uuid_generate_v4()::text) as id, version, tmp_sites.id as site_id, date_of_first_service, geom_point from tmp_sites
            union all
            select * from 
            (select md5(uuid_generate_v4()::text) as id, version, tmp_sites.id as site_id, date_of_first_service, geom_point from
            (
              select floor(random() * {num_sites} + 1)::integer as id
              from generate_series(1, {int(num_cells * 1.1)}) -- Preserve duplicates
            ) rands
            inner JOIN tmp_sites
            ON rands.id=tmp_sites.rid
            limit {num_cells - num_sites}) _) _
            ;
            CREATE INDEX ON tmp_cells (rid);
            """
        )
        trans.execute(
            "INSERT INTO infrastructure.cells (id, version, site_id, date_of_first_service, geom_point) SELECT id, version, site_id, date_of_first_service, geom_point FROM tmp_cells;"
        )
        print(f"Done. Runtime: {datetime.datetime.now() - start}")
        print(f"Generating {num_tacs} tacs.")
        start = datetime.datetime.now()
        trans.execute(
            f"""create table tacs as
        (select (row_number() over())::numeric(8, 0) as tac, 
        (ARRAY['Nokia', 'Huawei', 'Apple', 'Samsung', 'Sony', 'LG', 'Google', 'Xiaomi', 'ZTE'])[floor((random()*9 + 1))::int] as brand, 
        uuid_generate_v4()::text as model, 
        (ARRAY['Smart', 'Feature', 'Basic'])[floor((random()*3 + 1))::int] as  hnd_type 
        FROM generate_series(1, {num_tacs}));"""
        )
        trans.execute(
            "INSERT INTO infrastructure.tacs (id, brand, model, hnd_type) SELECT tac as id, brand, model, hnd_type FROM tacs;"
        )
        print(f"Done. Runtime: {datetime.datetime.now() - start}")
        print(f"Generating {num_subscribers} subscribers.")
        start = datetime.datetime.now()
        trans.execute(
            f"""
        create table subs as
        (select row_number() over() as id, md5(uuid_generate_v4()::text) as msisdn, md5(uuid_generate_v4()::text) as imei, 
        md5(uuid_generate_v4()::text) as imsi, floor(random() * {num_tacs} + 1)::numeric(8, 0) as tac 
        FROM generate_series(1, {num_subscribers}));
        CREATE INDEX on subs (id);
        ANALYZE subs;
        """
        )
        print(f"Done. Runtime: {datetime.datetime.now() - start}")

    start = datetime.datetime.now()
    print("Generating disaster.")
    with engine.begin() as trans:
        trans.execute(
            f"""CREATE TABLE available_cells AS 
                SELECT '2016-01-01'::date + rid*interval '1 day' as day,
                CASE WHEN ('2016-01-01'::date + rid*interval '1 day' BETWEEN '{disaster_start_date}'::date AND '{disaster_end_date}'::date) THEN
                    (array(select tmp_cells.id FROM tmp_cells INNER JOIN (SELECT * FROM geography.admin2 WHERE admin2pcod != '{pcode_to_knock_out}') _ ON ST_Within(geom_point, geom)))
                ELSE
                    (array(select tmp_cells.id FROM tmp_cells))
                END as cells
                FROM generate_series(0, {num_days}) as t(rid);"""
        )
    with engine.begin() as trans:
        trans.execute("CREATE INDEX ON available_cells (day);")
        trans.execute("ANALYZE available_cells;")
    print(f"Done. Runtime: {datetime.datetime.now() - start}")

    print("Assigning subscriber home regions.")
    start = datetime.datetime.now()
    with engine.begin() as trans:
        trans.execute(
            f"""CREATE TABLE homes AS (
               SELECT h.*, cells FROM
               (SELECT '2016-01-01'::date as home_date, id, admin3pcod as home_id FROM (
               SELECT *, floor(random() * (select count(distinct admin3pcod) from geography.admin3 RIGHT JOIN tmp_cells on ST_Within(geom_point, geom)) + 1)::integer as admin_id FROM subs
               ) subscribers
               LEFT JOIN
               (SELECT row_number() over() as admin_id, admin3pcod FROM 
               geography.admin3
               RIGHT JOIN
               tmp_cells ON ST_Within(geom_point, geom)
               ) geo
               ON geo.admin_id=subscribers.admin_id) h
               LEFT JOIN 
               (select admin3pcod, array_agg(id) as cells FROM tmp_cells LEFT JOIN geography.admin3 ON ST_Within(geom_point, geom) GROUP BY admin3pcod) c
               ON admin3pcod=home_id
               )
               """
        )
        trans.execute("CREATE INDEX ON homes (id);")
        trans.execute("CREATE INDEX ON homes (home_date);")
        trans.execute("CREATE INDEX ON homes (home_date, id);")
    for date in (
        datetime.date(2016, 1, 2) + datetime.timedelta(days=i) for i in range(num_days)
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
                        "%Y-%m-%d")}'::date as home_date, id, CASE WHEN (random() > 0.99 OR home_id = ANY((array(select admin3.admin3pcod FROM geography.admin3 WHERE admin2pcod = '{pcode_to_knock_out}')))) THEN admin3pcod ELSE home_id END as home_id FROM (
                                        SELECT *, floor(random() * (select count(distinct admin3pcod) from geography.admin3 RIGHT JOIN tmp_cells on ST_Within(geom_point, geom) WHERE admin2pcod!='{pcode_to_knock_out}') + 1)::integer as admin_id FROM homes
                                        WHERE home_date='{(date - datetime.timedelta(days=1)).strftime(
                        "%Y-%m-%d")}'::date
                                        ) subscribers
                                        LEFT JOIN
                                        (SELECT row_number() over() as admin_id, admin3pcod FROM 
                                        geography.admin3
                                        RIGHT JOIN
                                        tmp_cells ON ST_Within(geom_point, geom)
                                        WHERE admin2pcod!='{pcode_to_knock_out}'
                                        ) geo
                                        ON geo.admin_id=subscribers.admin_id) h
                                        LEFT JOIN 
                                                (select admin3pcod, array_agg(id) as cells FROM tmp_cells LEFT JOIN geography.admin3 ON ST_Within(geom_point, geom) GROUP BY admin3pcod) c
                                                ON admin3pcod=home_id
                                        """
                )
            else:
                trans.execute(
                    f"""INSERT INTO homes
                        SELECT h.*, cells FROM
                        (SELECT '{date.strftime(
                        "%Y-%m-%d")}'::date as home_date, id, CASE WHEN (random() > 0.99) THEN admin3pcod ELSE home_id END as home_id FROM (
                        SELECT *, floor(random() * (select count(distinct admin3pcod) from geography.admin3 RIGHT JOIN tmp_cells on ST_Within(geom_point, geom)) + 1)::integer as admin_id FROM homes
                        WHERE home_date='{(date - datetime.timedelta(days=1)).strftime("%Y-%m-%d")}'::date
                        ) subscribers
                        LEFT JOIN
                        (SELECT row_number() over() as admin_id, admin3pcod FROM 
                        geography.admin3
                        RIGHT JOIN
                        tmp_cells ON ST_Within(geom_point, geom)
                        ) geo
                        ON geo.admin_id=subscribers.admin_id) h
                        LEFT JOIN 
                                (select admin3pcod, array_agg(id) as cells FROM tmp_cells LEFT JOIN geography.admin3 ON ST_Within(geom_point, geom) GROUP BY admin3pcod) c
                                ON admin3pcod=home_id
                        """
                )
    with engine.begin() as trans:
        trans.execute("ANALYZE homes;")
    print(f"Done. Runtime: {datetime.datetime.now() - start}")

    start = datetime.datetime.now()
    print(f"Generating {num_subscribers * 5} interaction pairs.")
    with engine.begin() as trans:
        trans.execute(
            f"""CREATE TABLE interactions AS SELECT 
                row_number() over() as rid, callee_id, caller_id, caller.msisdn as caller_msisdn, 
                    caller.tac as caller_tac, caller.imsi as caller_imsi, caller.imei as caller_imei, 
                    callee.msisdn as callee_msisdn, callee.tac as callee_tac, 
                    callee.imsi as callee_imsi, callee.imei as callee_imei FROM
                (SELECT 
                floor(random() * {num_subscribers} + 1)::integer as caller_id, 
                floor(random() * {num_subscribers} + 1)::integer as callee_id FROM 
                generate_series(1, {num_subscribers * 5})) as pairs
                LEFT JOIN subs as caller ON pairs.caller_id = caller.id
                LEFT JOIN subs as callee ON pairs.callee_id = callee.id
            """
        )
        trans.execute("CREATE INDEX ON interactions (rid);")
        trans.execute("ANALYZE interactions;")
    print(f"Done. Runtime: {datetime.datetime.now() - start}")
    event_creation_sql = []
    sql = []
    for date in (
        datetime.date(2016, 1, 1) + datetime.timedelta(days=i) for i in range(num_days)
    ):
        table = date.strftime("%Y%m%d")
        end_date = (date + datetime.timedelta(days=1)).strftime("%Y-%m-%d")

        if num_calls > 0:
            event_creation_sql.append(
                (
                    f"Generating {num_calls} call events for {date}",
                    f"""
            CREATE TABLE call_evts_{table} AS
            SELECT ('{table}'::TIMESTAMPTZ + random() * interval '1 day') as start_time,
            round(random()*2600)::numeric as duration,
            uuid_generate_v4()::text as id, interactions.*,
            CASE WHEN (random() > 0.95) THEN
                available_cells.cells[floor(random()*array_length(available_cells.cells, 1) + 1)]
            ELSE
                 caller_homes.cells[floor(random()*array_length(caller_homes.cells, 1) + 1)]
            END as caller_cell,
            CASE WHEN (random() > 0.95) THEN
                available_cells.cells[floor(random()*array_length(available_cells.cells, 1) + 1)]
            ELSE
                 callee_homes.cells[floor(random()*array_length(callee_homes.cells, 1) + 1)]
            END as callee_cell
            FROM 
            (SELECT floor(random()*{num_subscribers * 5} + 1)::integer as rid FROM
            generate_series(1, {num_calls})) _
            LEFT JOIN
            interactions
            USING (rid)
            LEFT JOIN available_cells
            ON day='{table}'::date
            LEFT JOIN homes as caller_homes
            ON caller_homes.home_date='{table}'::date and caller_homes.id=interactions.caller_id
            LEFT JOIN homes as callee_homes
            ON callee_homes.home_date='{table}'::date and callee_homes.id=interactions.callee_id;

            CREATE TABLE events.calls_{table} AS 
            SELECT id, true as outgoing, start_time as datetime, duration, NULL::TEXT as network,
            caller_msisdn as msisdn, callee_msisdn as msisdn_counterpart, caller_cell as location_id,
            caller_imsi as imsi, caller_imei as imei, caller_tac as tac, NULL::NUMERIC as operator_code,
            NULL::NUMERIC as country_code
            FROM call_evts_{table}
            UNION ALL 
            SELECT id, false as outgoing, start_time as datetime, duration, NULL::TEXT as network,
            callee_msisdn as msisdn, caller_msisdn as msisdn_counterpart, callee_cell as location_id,
            callee_imsi as imsi, callee_imei as imei, callee_tac as tac, NULL::NUMERIC as operator_code,
            NULL::NUMERIC as country_code
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
            SELECT ('{table}'::TIMESTAMPTZ + random() * interval '1 day') as start_time,
            uuid_generate_v4()::text as id, interactions.*,
            CASE WHEN (random() > 0.95) THEN
                available_cells.cells[floor(random()*array_length(available_cells.cells, 1) + 1)]
            ELSE
                 caller_homes.cells[floor(random()*array_length(caller_homes.cells, 1) + 1)]
            END as caller_cell,
            CASE WHEN (random() > 0.95) THEN
                available_cells.cells[floor(random()*array_length(available_cells.cells, 1) + 1)]
            ELSE
                 callee_homes.cells[floor(random()*array_length(callee_homes.cells, 1) + 1)]
            END as callee_cell
            FROM 
            (SELECT floor(random()*{num_subscribers * 5} + 1)::integer as rid FROM
            generate_series(1, {num_sms})) _
            LEFT JOIN
            interactions
            USING (rid)
            LEFT JOIN available_cells
            ON day='{table}'::date
            LEFT JOIN homes as caller_homes
            ON caller_homes.home_date='{table}'::date and caller_homes.id=interactions.caller_id
            LEFT JOIN homes as callee_homes
            ON callee_homes.home_date='{table}'::date and callee_homes.id=interactions.callee_id;

            CREATE TABLE events.sms_{table} AS 
            SELECT id, true as outgoing, start_time as datetime, NULL::TEXT as network,
            caller_msisdn as msisdn, callee_msisdn as msisdn_counterpart, caller_cell as location_id,
            caller_imsi as imsi, caller_imei as imei, caller_tac as tac, NULL::NUMERIC as operator_code,
            NULL::NUMERIC as country_code
            FROM sms_evts_{table}
            UNION ALL 
            SELECT id, false as outgoing, start_time as datetime, NULL::TEXT as network,
            callee_msisdn as msisdn, caller_msisdn as msisdn_counterpart, callee_cell as location_id,
            callee_imsi as imsi, callee_imei as imei, callee_tac as tac, NULL::NUMERIC as operator_code,
            NULL::NUMERIC as country_code
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
            SELECT uuid_generate_v4()::text as id, ('{table}'::TIMESTAMPTZ + random() * interval '1 day') as datetime, 
            round(random() * 260)::numeric as duration, volume_upload + volume_download as volume_total, volume_upload,
            volume_download, msisdn, cell as location_id, imsi, imei, tac, 
            NULL::NUMERIC as operator_code, NULL::NUMERIC as country_code
            FROM
            (SELECT
            subs.msisdn, subs.imsi, subs.imei, subs.tac,
            round(random() * 100000)::numeric as volume_upload, 
            round(random() * 100000)::numeric as volume_download,
            CASE WHEN (random() > 0.95) THEN
                available_cells.cells[floor(random()*array_length(available_cells.cells, 1) + 1)]
            ELSE
                 caller_homes.cells[floor(random()*array_length(caller_homes.cells, 1) + 1)]
            END as cell
            FROM (SELECT floor(random()*{num_subscribers} + 1)::integer as id FROM
            generate_series(1, {num_mds})) _
            LEFT JOIN
            subs
            USING (id)
            LEFT JOIN available_cells
            ON day='{table}'::date
            LEFT JOIN homes as caller_homes
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
    for sub in ("calls", "sms", "mds"):
        if getattr(args, f"n_{sub}") > 0:
            for date in (
                datetime.date(2016, 1, 1) + datetime.timedelta(days=i)
                for i in range(num_days)
            ):
                table = date.strftime("%Y%m%d")
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
                    f"Updating availability for {sub}",
                    f"""
            INSERT INTO available_tables (table_name, has_locations, has_subscribers, has_counterparts) VALUES ('{sub}', true, true, true)
            ON conflict (table_name)
            DO UPDATE SET has_locations=EXCLUDED.has_locations, has_subscribers=EXCLUDED.has_subscribers, has_counterparts=EXCLUDED.has_counterparts;
            """,
                )
            )
        post_attach_sql.append((f"Analyzing {sub}", f"ANALYZE events.{sub};"))

    cleanup_sql = []
    for tbl in ("tmp_cells", "subs", "tacs", "available_cells", "homes"):
        cleanup_sql.append((f"Dropping {tbl}", f"DROP TABLE {tbl};"))

    def do_exec(args):
        msg, sql = args
        print(msg)
        started = datetime.datetime.now()
        with engine.begin() as trans:
            res = trans.execute(sql)
            try:
                print(f"{msg}: {res.fetchall()}")
            except ResourceClosedError:
                pass  # Nothing to do here

        print(f"Did: {msg}, runtime: {datetime.datetime.now() - started}")

    with ThreadPoolExecutor(cpu_count()) as tp:
        list(tp.map(do_exec, event_creation_sql))
        # list(tp.map(do_exec, sql))
        list(tp.map(do_exec, post_sql))
    for s in attach_sql:  # + cleanup_sql:
        do_exec(s)
    with ThreadPoolExecutor(cpu_count()) as tp:
        list(tp.map(do_exec, post_attach_sql))
    print(f"Total runtime: {datetime.datetime.now() - start_time}")
