# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import datetime
import sys
from datetime import datetime, timedelta, timezone

import pytest
from pathlib import Path
import jinja2

import importlib

from sqlalchemy import create_engine

from flowetl.operators.staging.event_columns import event_column_mappings


CSV_FOLDER = Path(__file__).parent.parent.parent / "mounts" / "files" / "static_csvs"
SQL_FOLDER = (
    Path(__file__).parent.parent.parent
    / "flowetl"
    / "flowetl"
    / "operators"
    / "staging"
    / "sql"
)
TEST_DATE = datetime(year=2021, month=9, day=29, tzinfo=timezone.utc)
TEST_DATE_STR = "20210929"
TEST_PARAMS = {"flowdb_csv_dir": str(CSV_FOLDER), "column_dict": event_column_mappings}

sql_env = jinja2.Environment(loader=jinja2.FileSystemLoader(SQL_FOLDER))


# From https://github.com/pytest-dev/pytest/issues/1872
# I don't like depending on a _pytest func, but can't see much choice...
@pytest.fixture(scope="session")
def monkeypatch_session():
    from _pytest.monkeypatch import MonkeyPatch

    m = MonkeyPatch()
    yield m
    m.undo()


@pytest.fixture(autouse=True, scope="session")
def airflow_home(tmpdir_factory, monkeypatch_session):
    tmpdir = tmpdir_factory.mktemp("data")
    monkeypatch_session.setenv("AIRFLOW_HOME", str(tmpdir))
    yield tmpdir


# @pytest.fixture(scope="function", autouse=True)
# def unload_airflow():
#     try:
#         yield
#     finally:
#         for module in list(sys.modules.keys()):
#             if "airflow" in module or "flowetl" in module:
#                 del sys.modules[module]


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


# Adapted from airflow.cli.commands.connection_command.py
@pytest.fixture()
def dummy_flowdb_conn(postgresql):
    # Create connection
    from airflow.utils.session import create_session
    from airflow.models.connection import Connection

    with postgresql as conn:
        conn.execute("CREATE EXTENSION IF NOT EXISTS file_fdw;")
        conn.execute("CREATE SCHEMA IF NOT EXISTS reduced;")
        conn.execute("CREATE SCHEMA IF NOT EXISTS staging;")
        conn.commit()
        testdb = Connection(
            conn_id="testdb",
            description="Temporary mock of flowdb",
            conn_type="postgresql",
            host=conn.info.host,
            login=conn.info.user,
            password=conn.info.password,
            port=conn.info.port,
            schema="tests",
        )
        with create_session() as session:
            session.add(testdb)

        yield conn

        with create_session() as session:
            session.delete(
                session.query(Connection).filter(Connection.conn_id == "testdb").one()
            )


@pytest.fixture()
def mounted_events_conn(dummy_flowdb_conn):
    mount_fill = sql_env.get_template("test_mount_all.sql")
    query = mount_fill.render(params=TEST_PARAMS, ds_nodash=TEST_DATE_STR)
    dummy_flowdb_conn.execute(query)
    yield dummy_flowdb_conn


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
