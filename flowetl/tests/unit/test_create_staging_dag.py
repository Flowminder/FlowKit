import datetime

import pytest

from flowetl.util import create_staging_dag, create_sighting_dag


@pytest.fixture()
def dummy_dag_env(monkeypatch):
    monkeypatch.setenv("SOURCE_TREE", "foo")
    monkeypatch.setenv("FLOWDB_CSV_DIR", "bar")


def test_create_staging_dag(dummy_dag_env):
    """Tests that create_staging_dag runs through. Does NOT test the DAG works!"""
    dag = create_staging_dag(
        start_date=datetime.datetime(2021, 9, 29, tzinfo=datetime.timezone.utc)
    )
    assert dag


def test_create_sighting_dag(dummy_dag_env):
    dag = create_sighting_dag(
        start_date=datetime.datetime(2021, 9, 29, tzinfo=datetime.timezone.utc)
    )
    assert dag
