from flowetl.staging.operators.create_and_fill_day_sightings_table import (
    CreateAndFillDaySightingsTable,
)
from flowetl.staging.operators.create_sightings_table import CreateSightingsTable
from flowetl.staging.operators.default_location_mapping import DefaultLocationMapping
from flowetl.staging.operators.append_sightings_to_main_table import (
    AppendSightingsToMainTable,
)
from flowetl.staging.operators.apply_mapping_to_staged_events import (
    ApplyMappingToStagedEvents,
)
from flowetl.staging.operators.create_and_fill_staging_table import (
    CreateAndFillStagingTable,
)

from airflow.operators.bash_operator import BashOperator

from staging.operators.example_location_mapping import ExampleLocationMapping


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


def test_create_and_fill_staging_table(mock_staging_dag, dummy_db_conn):
    run_task(
        CreateAndFillStagingTable(dag=mock_staging_dag),
        mock_staging_dag,
    )
    out = dummy_db_conn.execute("SELECT * FROM staging_table_2021_09_29")
    assert out.rowcount == 39


def test_append_sightings_to_main_table(mock_staging_dag, day_sightings_table_conn):
    # Needs sightings table to exist
    run_task(AppendSightingsToMainTable(dag=mock_staging_dag), mock_staging_dag)
    out = day_sightings_table_conn.execute("SELECT * FROM reduced.sightings")
    assert out.rowcount == 37


def test_apply_mapping_to_staged_events(
    mock_staging_dag, staged_data_conn, default_mapping_table_conn
):
    # Needs mapping table + staged events to exist
    run_task(ApplyMappingToStagedEvents(dag=mock_staging_dag), mock_staging_dag)


def test_create_and_fill_day_sightings_table(mock_staging_dag, sightings_table_conn):
    # Needs staging table + reduced sightings
    run_task(CreateAndFillDaySightingsTable(dag=mock_staging_dag), mock_staging_dag)
    out = sightings_table_conn.execute("SELECT * FROM reduced.sightings_2021_09_29")
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


def test_default_location_mapping(mock_staging_dag, staged_data_conn):
    run_task(DefaultLocationMapping(dag=mock_staging_dag), mock_staging_dag)


def test_example_location_mapping(mock_staging_dag, staged_data_conn):
    run_task(ExampleLocationMapping(dag=mock_staging_dag), mock_staging_dag)
