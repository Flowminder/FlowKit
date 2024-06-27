#!/usr/bin/env python
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import datetime
import os
import shutil
import pyarrow
import pyarrow.csv
import pyarrow.parquet
from contextlib import contextmanager
from pathlib import Path
from sqlalchemy import create_engine, text
from tempfile import NamedTemporaryFile

# From
# https://dba.stackexchange.com/questions/40441/get-all-partition-names-for-a-table
LIST_EVENT_TABLES_SQL = """
SELECT
child.relname       AS child_schema
FROM pg_inherits
JOIN pg_class parent ON pg_inherits.inhparent =parent.oid
JOIN pg_class child ON pg_inherits.inhrelid = child.oid
JOIN pg_namespace nmsp_parent ON nmsp_parent.oid = parent.relnamespace
JOIN pg_namespace nmsp_child ON nmsp_child.oid= child.relnamespace;
"""

DUMP_CSV_SQL = """
COPY events.{table_name} TO '{csv_fp}' DELIMITER ',' CSV HEADER;
DROP TABLE events.{table_name};
"""

DUMP_COL_TYPES_SQL = """
SELECT column_name, data_type
FROM information_schema.columns
WHERE table_name = '{table_name}';
"""


MOUNT_PARQUET_SQL = """
DROP TABLE events.{table_name};
CREATE FOREIGN TABLE events.{table_name} ()
INHERITS (events.{event_type})
SERVER parquet_srv 
OPTIONS(
filename '{parquet_path}'
);

--ALTER TABLE events.{event_type} 
    --ATTACH PARTITION events.{table_name}
    --FOR VALUES FROM ('{start_date}') TO ('{end_date}');

"""

PARQUET_COL_DTYPE_MAPPING = {
    "datetime": pyarrow.timestamp("ns", tz="UTC"),
    "duration": pyarrow.int64(),
    "outgoing": pyarrow.bool_(),
    "volume_total": pyarrow.int64(),
    "volume_upload": pyarrow.int64(),
    "volume_download": pyarrow.int64(),
    "msisdn": pyarrow.string(),
    "location_id": pyarrow.string(),
    "imsi": pyarrow.string(),
    "imei": pyarrow.string(),
    "tac": pyarrow.int64(),
    "network": pyarrow.string(),
    "operator_code": pyarrow.int64(),
    "country_code": pyarrow.int64(),
}


db_user = os.environ["POSTGRES_USER"]
db_name = os.environ["POSTGRES_DB"]
# Bind mount - location on host set in PARQUET_FOLDER env var
parquet_folder = Path("/parquet_files")
conn_str = f"postgresql://{db_user}@/{db_name}"
engine = create_engine(conn_str)


def get_event_table_list():
    with engine.connect() as conn:
        rows = conn.execute(text(LIST_EVENT_TABLES_SQL))
        return rows.scalars().all()


def convert_table_to_parquet(table_name):
    with NamedTemporaryFile() as csv_fp, engine.connect() as conn:
        event_type, date = table_name.split("_")
        start_date = datetime.datetime.strptime(date, "%Y%m%d")
        end_date = start_date + datetime.timedelta(days=1)

        cols = dump_to_csv(table_name, csv_fp.name)
        parquet_path = parquet_folder / table_name
        csv_to_parquet(csv_fp, str(parquet_path), cols)

        conn.execute(
            text(
                MOUNT_PARQUET_SQL.format(
                    table_name=table_name,
                    event_type=event_type,
                    parquet_path=parquet_path,
                    start_date=start_date.strftime("%Y-%m-%d"),
                    end_date=end_date.strftime("%Y-%m-%d"),
                )
            )
        )


def dump_to_csv(table_name, csv_path):
    print(f"Dumping {table_name} to {csv_path}")
    with engine.connect() as conn:
        cols = conn.execute(text(DUMP_COL_TYPES_SQL.format(table_name=table_name)))
        shutil.chown(csv_path, "postgres")
        conn.execute(text(DUMP_CSV_SQL.format(table_name=table_name, csv_fp=csv_path)))
        return {col_name: col_dtype for (col_name, col_dtype) in cols}


def csv_to_parquet(csv_path, parquet_path, cols):
    print(f"Converting {csv_path} to parquet at {parquet_path}")
    options = pyarrow.csv.ConvertOptions(
        column_types={
            k: v for k, v in PARQUET_COL_DTYPE_MAPPING.items() if k in cols.keys()
        },
        true_values=["t"],
        false_values=["f"],
    )
    table = pyarrow.csv.read_csv(csv_path, convert_options=options)
    pyarrow.parquet.write_table(table, parquet_path, compression="ZSTD")


if __name__ == "__main__":
    print(f"Creating parquet_folder at {PARQUET_PATH}")
    os.mkdir(PARQUET_PATH)
    for event_table in get_event_table_list():
        print(f"Converting {event_table} to parquet")
        convert_table_to_parquet(event_table)
