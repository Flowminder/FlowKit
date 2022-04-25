from datetime import timedelta
from time import sleep

import pytest

import pdb

import logging

from conftest import TEST_DATE, TEST_DATE_STR, TEST_PARAMS


@pytest.fixture()
def dag_env(monkeypatch):
    monkeypatch.setenv("FLOWETL_CSV_START_DATE", TEST_DATE_STR)


def test_create_staging_dag(clean_airflow_db, dummy_flowdb_conn, dag_env):
    from flowetl.util import create_staging_dag
    from airflow.exceptions import BackfillUnfinished
    from airflow.utils.state import DagRunState
    from airflow.utils.types import DagRunType
    from airflow.models import dagbag

    test_dagbag = dagbag.DagBag(
        include_examples=False,
    )
    dag = create_staging_dag(start_date=TEST_DATE, catchup=False)
    test_dagbag.bag_dag(dag, None)
    test_dagbag.sync_to_db()
    pass
    # globals()[dag.dag_id] = dag  # really, airflow?
    try:
        test_dagbag.dags[dag.dag_id].run(
            end_date=TEST_DATE,
            start_date=TEST_DATE,
            verbose=True,
            local=True,
            conf=TEST_PARAMS,
        )
        dagrun = dag.get_dagrun(execution_date=TEST_DATE)
    except BackfillUnfinished:
        dagrun = dag.get_dagrun(execution_date=TEST_DATE)
        for ti in dagrun.get_task_instances():
            print(ti.log)
            pass
        pytest.fail()

    timeout_count = 0
    while dagrun.get_state() == DagRunState.RUNNING:
        dagrun.update_state()
        timeout_count += 1
        if timeout_count >= 10000000:
            break
        pass
    assert dagrun.get_state() == DagRunState.SUCCESS
    record_count = dummy_flowdb_conn.execute(
        f"SELECT count(*) FROM etl.staging_{TEST_DATE_STR}"
    ).fetchall[0][0]
    assert record_count == 37
