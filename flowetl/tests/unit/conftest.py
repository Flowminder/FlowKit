# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import datetime
import sys

from airflow import DAG
import pytest
from pathlib import Path
import jinja2


CSV_FOLDER = Path(__file__).parent.parent / "static_csvs"
SQL_FOLDER = (
    Path(__file__).parent.parent.parent
    / "flowetl"
    / "flowetl"
    / "staging"
    / "operators"
    / "sql"
)
TEST_DATE = datetime.datetime(year=2021, month=9, day=29)
TEST_DATE_STR = "2021_09_29"
TEST_PARAMS = {"date": TEST_DATE_STR, "csv_dir": str(CSV_FOLDER)}

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


# In hindsight, these could have been a factory or partial. Ho hum.
@pytest.fixture()
def dummy_db_conn(postgresql_db, monkeypatch):
    postgresql_db.install_extension("file_fdw")
    postgresql_db.create_schema("reduced")
    with postgresql_db.engine.connect() as conn:
        monkeypatch.setenv("AIRFLOW_CONN_TESTDB", str(conn.engine.url))
        yield conn


@pytest.fixture()
def staged_data_conn(dummy_db_conn):
    st_fill = sql_env.get_template("create_and_fill_staging_table.sql")
    query = st_fill.render(params=TEST_PARAMS)
    dummy_db_conn.execute(query)
    yield dummy_db_conn


@pytest.fixture()
def sightings_table_conn(staged_data_conn):
    sight_setup = sql_env.get_template("create_sightings_table.sql")
    query = sight_setup.render(params=TEST_PARAMS)
    staged_data_conn.execute(query)
    yield staged_data_conn


@pytest.fixture()
def default_mapping_table_conn(staged_data_conn):
    map_setup = sql_env.get_template("default_location_mapping.sql")
    query = map_setup.render(params=TEST_PARAMS)
    staged_data_conn.execute(query)
    yield staged_data_conn


@pytest.fixture()
def day_sightings_table_conn(sightings_table_conn, staged_data_conn):
    day_sight_setup = sql_env.get_template("create_and_fill_day_sightings_table.sql")
    query = day_sight_setup.render(params=TEST_PARAMS)
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
        },
        params={
            "date": datetime.datetime(2021, 9, 29).strftime("%Y_%m_%d"),
            "csv_dir": str(Path(__file__).parent.parent / "static_csvs"),
        },
        schedule_interval=datetime.timedelta(days=1),
        template_searchpath="/home/john/projects/flowkit_1/FlowKit/flowetl/flowetl/flowetl/staging/",
    )
