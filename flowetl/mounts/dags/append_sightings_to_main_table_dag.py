from airflow import DAG
from datetime import datetime

from airflow.operators.dummy_operator import DummyOperator

with DAG(
    dag_id="load_records_from_staging_dag",
    start_date=datetime(2016, 3, 1),
) as dag:
    from flowetl.staging.operators.create_and_fill_day_sightings_table import (
        CreateAndFillDaySightingsTable,
    )
    from flowetl.staging.operators.create_sightings_table import CreateSightingsTable
    from flowetl.staging.operators.default_location_mapping import (
        DefaultLocationMapping,
    )
    from flowetl.staging.operators.append_sightings_to_main_table import (
        AppendSightingsToMainTable,
    )
    from flowetl.staging.operators.apply_mapping_to_staged_events import (
        ApplyMappingToStagedEvents,
    )
    from flowetl.staging.operators.create_and_fill_staging_table import (
        CreateAndFillStagingTable,
    )

    create_and_fill_staging_table = CreateAndFillStagingTable()
    location_mapping = DefaultLocationMapping()
    apply_mapping_to_staged_events = ApplyMappingToStagedEvents()
    create_sightings_table = CreateSightingsTable()
    create_day_sightings_table = CreateAndFillDaySightingsTable()
    append_sightings = AppendSightingsToMainTable()

    append_sightings << [create_day_sightings_table, create_sightings_table]
    (
        create_day_sightings_table
        << apply_mapping_to_staged_events
        << location_mapping
        << create_and_fill_staging_table
    )


with DAG(dag_id="test_dag", start_date=datetime(2016, 1, 1)) as dag_2:
    op = DummyOperator(task_id="foo")
