import datetime

from flowetl.util import create_staging_dag


def test_create_staging_dag(monkeypatch):
    """Tests that create_staging_dag runs through. Does NOT test the DAG works!"""
    monkeypatch.setenv("SOURCE_TREE", "foo")
    monkeypatch.setenv("FLOWDB_CSV_DIR", "bar")
    dag = create_staging_dag(datetime.datetime(2021, 9, 29), ["sms", "topup"])
    assert dag
