import pytest
import sys
from conftest import TEST_DATE, TEST_PARAMS, SQL_FOLDER
from datetime import timedelta, datetime
import importlib
from flowetl.operators.staging.event_columns import event_column_mappings


@pytest.fixture(autouse=True, scope="module")
def clean_airflow_db(monkeypatch_session, airflow_home):
    from airflow.utils import db

    monkeypatch_session.setenv("AIRFLOW__CORE__LOAD_EXAMPLES", "False")
    db.initdb()


def run_task(task):

    # Adapted from https://airflow.apache.org/docs/apache-airflow/stable/best-practices.html#unit-tests

    from airflow import DAG
    from airflow.utils.state import DagRunState
    from airflow.utils.types import DagRunType

    dag = DAG(
        f"test_{task.task_id}",
        default_args={
            "owner": "airflow",
            "start_date": datetime(2021, 9, 29),
            "column_dict": event_column_mappings,
        },
        params=TEST_PARAMS,
        schedule_interval=timedelta(days=1),
        template_searchpath=str(SQL_FOLDER),
        is_paused_upon_creation=True,
    )

    dag.add_task(task)
    dagrun = dag.create_dagrun(
        run_id=f"test_{task.task_id}_run",
        state=DagRunState.RUNNING,
        execution_date=TEST_DATE,
        data_interval=(TEST_DATE, TEST_DATE + timedelta(days=1)),
        start_date=TEST_DATE + timedelta(days=1),
        run_type=DagRunType.MANUAL,
    )
    ti = dagrun.get_task_instance(task_id=task.task_id)
    ti.task = dag.get_task(task_id=task.task_id)
    ti.run(ignore_ti_state=True)
    assert ti.state == DagRunState.SUCCESS
    dag.clear()


def test_mock_dag():
    from airflow.operators.bash import BashOperator

    run_task(
        BashOperator(
            task_id="bash_test", bash_command="echo $PWD", start_date=TEST_DATE
        ),
    )


def test_mount_event_table(dummy_flowdb_conn):
    from airflow.providers.postgres.operators.postgres import PostgresOperator
    from flowetl.operators.staging.event_columns import event_column_mappings

    sms_operator = PostgresOperator(
        sql="mount_event.sql",
        task_id="mount_event",
        params={"event_type": "sms"},
        postgres_conn_id="testdb",
        start_date=TEST_DATE,
    )
    run_task(sms_operator)
    columns = dummy_flowdb_conn.execute(
        f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'sms_table_20210929'
        """
    ).fetchall()
    returned_column_names = [row[0].upper() for row in columns]
    assert all(
        [
            column_name in returned_column_names
            for column_name in event_column_mappings["sms"].keys()
        ]
    )


def test_create_and_fill_staging_table(mounted_events_conn):
    from airflow.providers.postgres.operators.postgres import PostgresOperator

    fill_operator = PostgresOperator(
        sql="stage_events.sql",
        task_id="stage_events",
        params={"event_types": ["call", "sms"]},
        postgres_conn_id="testdb",
        start_date=TEST_DATE,
    )
    run_task(
        fill_operator,
    )
    out = mounted_events_conn.execute("SELECT * FROM staging.staging_table_20210929")
    assert out.rowcount == 20


def test_append_sightings_to_main_table(day_sightings_table_conn):
    from airflow.providers.postgres.operators.postgres import PostgresOperator

    # Needs sightings table to exist
    append_operator = PostgresOperator(
        sql="append_sightings_to_main_table.sql",
        task_id="append_sightings_to_main_table",
        postgres_conn_id="testdb",
        start_date=TEST_DATE,
    )
    run_task(append_operator)
    out = day_sightings_table_conn.execute("SELECT * FROM reduced.sightings")
    assert out.rowcount == 37


def test_create_and_fill_day_sightings_table(sightings_table_conn):
    from airflow.providers.postgres.operators.postgres import PostgresOperator

    # Needs staging table + reduced sightings
    day_sightings = PostgresOperator(
        sql="create_and_fill_day_sightings_table.sql",
        task_id="create_and_fill_day_sightings_table",
        postgres_conn_id="testdb",
        start_date=TEST_DATE,
    )
    run_task(day_sightings)
    out = sightings_table_conn.execute("SELECT * FROM reduced.sightings_20210929")
    assert out.rowcount == 37


def test_create_sightings_table(dummy_flowdb_conn):
    from airflow.providers.postgres.operators.postgres import PostgresOperator

    create_sightings = PostgresOperator(
        sql="create_sightings_table.sql",
        task_id="create_sightings_table",
        postgres_conn_id="testdb",
        start_date=TEST_DATE,
    )
    run_task(create_sightings)
    out = dummy_flowdb_conn.execute("SELECT * FROM reduced.sightings").fetchall()
    assert len(out) == 0
    target_column_names = [
        "sighting_date",
        "sub_id",
        "sighting_id",
        "location_id",
        "event_times",
        "event_types",
    ]
    columns = dummy_flowdb_conn.execute(
        f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'sightings' AND table_schema = 'reduced'
        """
    ).fetchall()
    returned_column_names = [row[0] for row in columns]
    assert all(
        [column_name in returned_column_names for column_name in target_column_names]
    )


def test_staging_cleanup(staged_data_conn):
    from airflow.providers.postgres.operators.postgres import PostgresOperator

    test_date = "20210929"
    staging_tables = [
        f"staging_table_{test_date}",
        f"call_table_{test_date}",
        f"sms_table_{test_date}",
        f"location_table_{test_date}",
        f"mds_table_{test_date}",
        f"topup_table_{test_date}",
    ]

    # Check staging tables exist
    table_list = staged_data_conn.execute(
        f"""
        SELECT table_name
        FROM information_schema.tables
        """
    )
    before_table_names = [row[0] for row in table_list]
    assert all(st in before_table_names for st in staging_tables)

    cleanup = PostgresOperator(
        sql="cleanup_staging_table.sql",
        task_id="cleanup_staging_table",
        postgres_conn_id="testdb",
        start_date=TEST_DATE,
    )
    run_task(cleanup)

    # Check staging tables have been cleared up
    table_list = staged_data_conn.execute(
        f"""
        SELECT table_name
        FROM information_schema.tables
        """
    ).fetchall()
    after_table_names = [row[0] for row in table_list]
    assert all(st not in after_table_names for st in staging_tables)
