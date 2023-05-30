#!/usr/bin/env python
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import datetime
import os
import shutil
import pyarrow.csv
import pyarrow.parquet
from contextlib import contextmanager
from pathlib import Path
from sqlalchemy import create_engine, text
from tempfile import NamedTemporaryFile

# From
# https://dba.stackexchange.com/questions/40441/get-all-partition-names-for-a-table
EVENT_TYPES = ["sms", "calls", "data"]

LIST_EVENT_TABLES_SQL = """
SELECT
child.relname       AS child_schema
FROM pg_inherits
JOIN pg_class parent ON pg_inherits.inhparent =parent.oid
JOIN pg_class child ON pg_inherits.inhrelid = child.oid
JOIN pg_namespace nmsp_parent ON nmsp_parent.oid = parent.relnamespace
JOIN pg_namespace nmsp_child ON nmsp_child.oid= child.relnamespace
"""

DUMP_CSV_SQL = """
COPY events.{table_name} TO '{csv_fp}' DELIMITER ',' CSV HEADER;
--DROP TABLE events.{table_name};
"""

MOUNT_PARQUET_SQL = """
CREATE FOREIGN TABLE IF NOT EXISTS events.parq_{table_name}
INHERITS events.{event_type}
SERVER parquet_srv 
OPTIONS(
    filename '{parquet_path}',
    sorted 'msisdn start_time'
);

ALTER TABLE events.parq_{table_name} NO INHERIT events.{event_type};

"""

PARQUET_FOLDER = Path("/parquet_files")

db_user = os.environ["POSTGRES_USER"]
db_name = os.environ["POSTGRES_DB"]
conn_str = f"postgresql://{db_user}@/{db_name}"
engine = create_engine(conn_str)


def get_event_table_list():
    with engine.connect() as conn:
        rows = conn.execute(text(LIST_EVENT_TABLES_SQL))
        print(rows)
        return rows.scalars().all()


def convert_table_to_parquet(table_name):
    with (NamedTemporaryFile() as csv_fp, engine.connect() as conn):
        event_type, date = table_name.split("_")
        shutil.chown(csv_fp.name, "postgres")
        conn.execute(
            text(DUMP_CSV_SQL.format(table_name=table_name, csv_fp=csv_fp.name))
        )
        parquet_path = PARQUET_FOLDER / table_name
        csv_to_parquet(csv_fp, str(parquet_path))
        conn.execute(
            text(
                MOUNT_PARQUET_SQL.format(
                    table_name=table_name,
                    event_type=event_type,
                    parquet_path=parquet_path,
                )
            )
        )


def csv_to_parquet(csv_path, parquet_path):
    table = pyarrow.csv.read_csv(csv_path)
    pyarrow.parquet.write_table(table, parquet_path)


if __name__ == "__main__":
    for event_table in get_event_table_list():
        convert_table_to_parquet(event_table)
