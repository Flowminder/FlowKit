from flowetl.operators.staging.create_and_fill_day_sightings_table import (
    CreateAndFillDaySightingsTable,
)
from flowetl.operators.staging.create_sightings_table import CreateSightingsTable
from flowetl.operators.staging.append_sightings_to_main_table import (
    AppendSightingsToMainTable,
)

from flowetl.operators.staging.create_and_fill_staging_table import (
    CreateAndFillStagingTable,
)
from flowetl.operators.staging.cleanup_staging_table import CleanupStagingTable

from airflow.operators.bash_operator import BashOperator

from operators.staging.mount_event_operator_factory import create_mount_event_operator
from operators.staging.event_columns import event_column_mappings


def run_task(task, dag):
    dag.clear()
    task.run(
        start_date=dag.default_args["start_date"],
        end_date=dag.default_args["start_date"],
    )


def test_mock_dag(mock_staging_dag):
    run_task(
        BashOperator(
            task_id="bash_test", bash_command="echo $PWD", dag=mock_staging_dag
        ),
        mock_staging_dag,
    )


def test_mount_event_table(mock_staging_dag, dummy_db_conn):
    sms_operator = create_mount_event_operator(event_type="sms")
    run_task(sms_operator(dag=mock_staging_dag), mock_staging_dag)
    columns = dummy_db_conn.execute(
        f"""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_name = 'sms_table_20210929'
        """
    ).fetchall()
    returned_column_names = [row["column_name"].upper() for row in columns]
    assert all(
        [
            column_name in returned_column_names
            for column_name in event_column_mappings["sms"].keys()
        ]
    )


def test_create_and_fill_staging_table(mock_staging_dag, mounted_events_conn):
    run_task(
        CreateAndFillStagingTable(
            dag=mock_staging_dag, event_type_list=["call", "sms"]
        ),
        mock_staging_dag,
    )
    out = mounted_events_conn.execute("SELECT * FROM staging_table_20210929")
    assert out.rowcount == 20


def test_append_sightings_to_main_table(mock_staging_dag, day_sightings_table_conn):
    # Needs sightings table to exist
    run_task(AppendSightingsToMainTable(dag=mock_staging_dag), mock_staging_dag)
    out = day_sightings_table_conn.execute("SELECT * FROM reduced.sightings")
    assert out.rowcount == 37


def test_create_and_fill_day_sightings_table(mock_staging_dag, sightings_table_conn):
    # Needs staging table + reduced sightings
    run_task(CreateAndFillDaySightingsTable(dag=mock_staging_dag), mock_staging_dag)
    out = sightings_table_conn.execute("SELECT * FROM reduced.sightings_20210929")
    assert out.rowcount == 37  # two merged


def test_create_sightings_table(mock_staging_dag, dummy_db_conn):
    run_task(CreateSightingsTable(dag=mock_staging_dag), mock_staging_dag)
    out = dummy_db_conn.execute("SELECT * FROM reduced.sightings")
    assert out.rowcount == 0
    assert out.keys() == [
        "sighting_date",
        "sub_id",
        "sighting_id",
        "location_id",
        "event_times",
        "event_types",
    ]


def test_staging_cleanup(mock_staging_dag, staged_data_conn):
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
    before_table_names = [row["table_name"] for row in table_list]
    assert all(st in before_table_names for st in staging_tables)

    run_task(CleanupStagingTable(dag=mock_staging_dag), mock_staging_dag)

    # Check staging tables have been cleared up
    table_list = staged_data_conn.execute(
        f"""
        SELECT table_name
        FROM information_schema.tables
        """
    ).fetchall()
    after_table_names = [row["table_name"] for row in table_list]
    assert all(st not in after_table_names for st in staging_tables)
