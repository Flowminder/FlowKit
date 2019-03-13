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
import pandas as pd
import argparse
import datetime


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
    "--output-root-dir",
    type=str,
    default="",
    help="Root directory under which output .sql is stored (in appropriate subfolders).",
)


def write_ingest_sql(sql, output_root_dir):
    """
    Write to `{output_root_dir}/sql/syntheticdata/ingest_calls.sql` an ingestion
    script that wraps the list of table creation statements provided in tables.

    Parameters
    ----------
    sql: list of str
        List of SQL strings, each of which is a table creation statement.
    """
    sql = "\n".join(sql)
    ingest_sql = f"""
        BEGIN;
        DELETE FROM events.calls;
        {sql}
        COMMIT;
    """
    output_dir = os.path.join(output_root_dir, "sql", "syntheticdata")
    os.makedirs(output_dir, exist_ok=True)
    with open(f"{output_dir}/ingest_calls.sql", "w") as fout:
        fout.write(ingest_sql)


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

    sql = []
    sql.append(
        f"""CREATE TEMPORARY TABLE tmp_sites as 
        select row_number() over() as rid, md5(uuid_generate_v4()::text) as id, 
        0 as version, (date '2015-01-01' + random() * interval '1 year')::date as date_of_first_service,
        (p).geom as geom_point from (select st_dumppoints(ST_GeneratePoints(geom, {num_sites})) as p from geography.admin0) _;"""
    )
    sql.append(
        "INSERT INTO infrastructure.sites (id, version, date_of_first_service, geom_point) SELECT id, version, date_of_first_service, geom_point FROM tmp_sites;"
    )

    sql.append(
        f"""CREATE TEMPORARY TABLE tmp_cells as
    select md5(uuid_generate_v4()::text) as id, version, tmp_sites.id as site_id, date_of_first_service, geom_point from tmp_sites
    union all
    select md5(uuid_generate_v4()::text) as id, version, tmp_sites.id as site_id, date_of_first_service, geom_point from
    (
      select floor(random() * {num_sites} + 1)::integer as id
      from generate_series(1, {int(num_cells * 1.1)}) -- Preserve duplicates
    ) rands
    inner JOIN tmp_sites
    ON rands.id=tmp_sites.rid
    limit {num_cells - num_sites};"""
    )
    sql.append(
        "INSERT INTO infrastructure.cells (id, version, site_id, date_of_first_service, geom_point) SELECT id, version, site_id, date_of_first_service, geom_point FROM tmp_cells;"
    )

    sql.append(
        f"""create temporary table tacs as
    (select row_number() over() as tac, 
    (ARRAY['Nokia', 'Huawei', 'Apple', 'Samsung', 'Sony', 'LG', 'Google', 'Xiaomi', 'ZTE'])[floor((random()*9 + 1))::int] as brand, 
    uuid_generate_v4()::text as model, 
    (ARRAY['Smart', 'Feature', 'Basic'])[floor((random()*3 + 1))::int] as  hnd_type 
    FROM generate_series(1, {num_tacs}));"""
    )
    sql.append(
        "INSERT INTO infrastructure.tacs (id, brand, model, hnd_type) SELECT tac as id, brand, model, hnd_type FROM tacs;"
    )

    sql.append(
        f"""
    create temporary table subs as
    (select row_number() over() as id, md5(uuid_generate_v4()::text) as msisdn, md5(uuid_generate_v4()::text) as imei, 
    md5(uuid_generate_v4()::text) as imsi, floor(random() * {num_tacs} + 1)::integer as tac 
    FROM generate_series(1, {num_subscribers}));
    CREATE INDEX on subs (id);
    """
    )

    for date in pd.date_range("2016-01-01", periods=num_days):
        table = date.strftime("%Y%m%d")
        end_date = (date + datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        # calls
        if num_calls > 0:
            sql.append(
                f"""
            CREATE TABLE IF NOT EXISTS events.calls_{table} (
                                CHECK ( datetime >= '{table}'::TIMESTAMPTZ
                                AND datetime < '{end_date}'::TIMESTAMPTZ)
                            ) INHERITS (events.calls);
        
            with calls as 
            (SELECT uuid_generate_v4()::text as id, caller.msisdn as caller_msisdn, caller.tac as caller_tac, caller.imsi as caller_imsi, caller.imei as imei, callee.msisdn as callee_msisdn, callee.tac as callee_tac, callee.imsi as callee_imsi, callee.imei as imei, caller_cell, callee_cell, start_time, duration FROM
            (select *, case when caller_id != callee_id then floor(random() * {num_cells} + 1)::integer ELSE caller_cell END as callee_cell, (date '{table}' + random() * interval '1 day') as start_time, round(random()*2600)::integer as duration FROM
            (select *, floor(random() * {num_cells} + 1)::integer as caller_cell FROM
            (select floor(random() * {num_subscribers} + 1)::integer as caller_id, floor(random() * {num_subscribers} + 1)::integer as callee_id FROM generate_series(1, {num_calls})) _) _) calls
            LEFT JOIN subs as caller ON calls.caller_id = caller.id
            LEFT JOIN subs as callee ON calls.callee_id = callee.id)
        
            INSERT INTO events.calls_{table} (id, outgoing, duration, datetime, msisdn, tac, imsi, location_id, msisdn_counterpart)
                SELECT * FROM (
                    SELECT id, true as outgoing, duration, start_time as datetime, caller_msisdn as msisdn, caller_tac as tac, caller_imsi as imsi, caller_cell as location_id, callee_msisdn as msisdn_counterpart 
                FROM calls
                UNION ALL
                SELECT id, false as outgoing, duration, start_time as datetime, callee_msisdn as msisdn, callee_tac as tac, callee_imsi as imsi, callee_cell as location_id, caller_msisdn as msisdn_counterpart
                FROM calls) _ ORDER BY datetime ASC, msisdn;
        
            CREATE INDEX ON events.calls_{table} (msisdn);
            CREATE INDEX ON events.calls_{table} (msisdn_counterpart);
            CREATE INDEX ON events.calls_{table} (tac);
            CREATE INDEX ON events.calls_{table} (location_id);
            CREATE INDEX ON events.calls_{table} (datetime);
            """
            )
        if num_sms > 0:
            sql.append(
                f"""
            CREATE TABLE IF NOT EXISTS events.sms_{table} (
                                        CHECK ( datetime >= '{table}'::TIMESTAMPTZ
                                        AND datetime < '{end_date}'::TIMESTAMPTZ)
                                    ) INHERITS (events.sms);
            with sms as
            (SELECT uuid_generate_v4()::text as id, caller.msisdn as caller_msisdn, caller.tac as caller_tac, caller.imsi as caller_imsi, caller.imei as imei, callee.msisdn as callee_msisdn, callee.tac as callee_tac, callee.imsi as callee_imsi, callee.imei as imei, caller_cell, callee_cell, start_time FROM
            (select *, case when caller_id != callee_id then round(random() * {num_cells} + 1)::integer ELSE caller_cell END as callee_cell, (date '{table}' + random() * interval '1 day') as start_time FROM
            (select *, round(random() * {num_cells} + 1)::integer as caller_cell FROM
            (select floor(random() * {num_subscribers} + 1)::integer as caller_id, floor(random() * {num_subscribers} + 1)::integer as callee_id FROM generate_series(1, {num_sms})) _) _) calls
            LEFT JOIN subs as caller ON calls.caller_id = caller.id
            LEFT JOIN subs as callee ON calls.callee_id = callee.id)
    
            INSERT INTO events.sms_{table} (id, outgoing, datetime, msisdn, tac, imsi, location_id, msisdn_counterpart)
    
            SELECT * FROM (SELECT id, true as outgoing, start_time as datetime, caller_msisdn as msisdn, caller_tac as tac, caller_imsi as imsi, caller_cell as location_id, callee_msisdn as msisdn_counterpart
            FROM sms
            UNION ALL
            SELECT id, false as outgoing, start_time as datetime, callee_msisdn as msisdn, callee_tac as tac, callee_imsi as imsi, callee_cell as location_id, caller_msisdn as msisdn_counterpart
            FROM sms) _ ORDER BY datetime ASC, msisdn;
    
            CREATE INDEX ON events.sms_{table} (msisdn);
            CREATE INDEX ON events.sms_{table} (tac);
            CREATE INDEX ON events.sms_{table} (location_id);
            CREATE INDEX ON events.sms_{table} (datetime);
    
            """
            )
        if num_mds > 0:
            sql.append(
                f"""
            CREATE TABLE IF NOT EXISTS events.mds_{table} (
                                        CHECK ( datetime >= '{table}'::TIMESTAMPTZ
                                        AND datetime < '{end_date}'::TIMESTAMPTZ)
                                    ) INHERITS (events.mds);
            
            INSERT INTO events.mds_{table} (msisdn, tac, imsi, imei, volume_upload, volume_download, volume_total, location_id, datetime, duration)
            SELECT msisdn, tac, imsi, imei, volume_upload, volume_download, 
            volume_upload + volume_download as volume_total, 
            floor(random() * {num_cells} + 1)::integer as location_id, 
            (date '{table}' + random() * interval '1 day') as datetime, round(random() * 260)::integer as duration FROM
            (
                select floor(random() * {num_subscribers} + 1)::integer as id, round(random() * 100000)::integer as volume_upload, 
                round(random() * 100000)::integer as volume_download FROM generate_series(1, {num_mds})) _
            LEFT JOIN
            subs
            USING (id) ORDER BY datetime ASC, msisdn;
    
            CREATE INDEX ON events.mds_{table} (msisdn);
            CREATE INDEX ON events.mds_{table} (tac);
            CREATE INDEX ON events.mds_{table} (location_id);
            CREATE INDEX ON events.mds_{table} (datetime);
            """
            )

    for sub in ("calls", "sms", "mds"):
        if args[f"n_{sum}"] > 0:
            for date in pd.date_range("2016-01-01", periods=num_days):
                table = date.strftime("%Y%m%d")
                if args.cluster:
                    sql.append(
                        f"CLUSTER events.{sub}_{table} USING {sub}_{table}_msisdn_idx;"
                    )
                sql.append(f"ANALYZE events.{sub}_{table};")
            sql.append(
                f"""
            INSERT INTO available_tables (table_name, has_locations, has_subscribers, has_counterparts) VALUES ('{sub}', true, true, true)
            ON conflict (table_name)
            DO UPDATE SET has_locations=EXCLUDED.has_locations, has_subscribers=EXCLUDED.has_subscribers, has_counterparts=EXCLUDED.has_counterparts;
            """
            )
        sql.append(f"ANALYZE events.{sub};")
    write_ingest_sql(sql, args.output_root_dir)
