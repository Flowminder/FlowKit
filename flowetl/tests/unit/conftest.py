# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import datetime
import os
import shutil
import sys
from datetime import datetime, timedelta

from airflow import DAG
import pytest
from pathlib import Path
import jinja2

from operators.staging.event_columns import event_column_mappings

CSV_FOLDER = Path(__file__).parent.parent.parent / "mounts" / "files" / "static_csvs"
SQL_FOLDER = (
    Path(__file__).parent.parent.parent
    / "flowetl"
    / "flowetl"
    / "operators"
    / "staging"
    / "sql"
)
TEST_DATE = datetime.datetime(year=2021, month=9, day=29)
TEST_DATE_STR = "20210929"
TEST_PARAMS = {"flowdb_csv_dir": str(CSV_FOLDER), "column_dict": event_column_mappings}

sql_env = jinja2.Environment(loader=jinja2.FileSystemLoader(SQL_FOLDER))


@pytest.fixture(autouse=True)
def airflow_home(tmpdir, monkeypatch):
    monkeypatch.setenv("AIRFLOW_HOME", str(tmpdir))
    yield tmpdir


@pytest.fixture(autouse=True)
def unload_airflow():
    try:
        yield
    finally:
        for module in list(sys.modules.keys()):
            if "airflow" in module or "flowetl" in module:
                del sys.modules[module]


@pytest.fixture()
def mock_basic_dag():
    from airflow import DAG

    dag = DAG(
        "test_dag",
        default_args={
            "owner": "airflow",
            "start_date": datetime(2021, 9, 29),
        },
        schedule_interval=timedelta(days=1),
    )
    yield dag


# In hindsight, these could have been a factory or partial. Ho hum.
@pytest.fixture()
def dummy_db_conn(postgresql_db, monkeypatch):
    postgresql_db.install_extension("file_fdw")
    postgresql_db.create_schema("reduced")
    with postgresql_db.engine.connect() as conn:
        monkeypatch.setenv("AIRFLOW_CONN_TESTDB", str(conn.engine.url))
        yield conn


@pytest.fixture()
def mounted_events_conn(dummy_db_conn):
    mount_fill = sql_env.get_template("test_mount_all.sql")
    query = mount_fill.render(params=TEST_PARAMS, ds_nodash=TEST_DATE_STR)
    dummy_db_conn.execute(query)
    yield dummy_db_conn


@pytest.fixture()
def staged_data_conn(mounted_events_conn):
    st_fill = sql_env.get_template("test_stage_all.sql")
    query = st_fill.render(params=TEST_PARAMS, ds_nodash=TEST_DATE_STR)
    mounted_events_conn.execute(query)
    yield mounted_events_conn


@pytest.fixture()
def sightings_table_conn(staged_data_conn):
    sight_setup = sql_env.get_template("create_sightings_table.sql")
    query = sight_setup.render(params=TEST_PARAMS, ds_nodash=TEST_DATE_STR)
    staged_data_conn.execute(query)
    yield staged_data_conn


@pytest.fixture()
def day_sightings_table_conn(sightings_table_conn, staged_data_conn):
    day_sight_setup = sql_env.get_template("create_and_fill_day_sightings_table.sql")
    query = day_sight_setup.render(params=TEST_PARAMS, ds_nodash=TEST_DATE_STR)
    staged_data_conn.execute(query)
    yield sightings_table_conn


@pytest.fixture()
def real_airflow_conn(monkeypatch):
    monkeypatch.setenv(
        "AIRFLOW__CORE__SQL_ALCHEMY_CONN",
        "postgres://flowetl:flowetl@localhost:9001/flowetl",
    )


# From https://godatadriven.com/blog/testing-and-debugging-apache-airflow/
@pytest.fixture()
def mock_staging_dag(real_airflow_conn, dummy_db_conn):

    return DAG(
        "test_dag",
        default_args={
            "owner": "airflow",
            "start_date": datetime.datetime(2021, 9, 29),
            "postgres_conn_id": "testdb",
            "column_dict": event_column_mappings,
        },
        params=TEST_PARAMS,
        schedule_interval=datetime.timedelta(days=1),
        template_searchpath=str(SQL_FOLDER),
    )
