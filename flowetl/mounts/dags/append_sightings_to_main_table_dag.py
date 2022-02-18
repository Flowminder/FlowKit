import os
from pathlib import Path

from airflow import DAG
from datetime import datetime

from airflow.operators.dummy_operator import DummyOperator

# Hack to make sure the template path follows the import around
from flowetl.operators.staging.sql import __file__ as template_path

template_folder = (
    "/" + os.getenv("SOURCE_TREE") + "/flowetl/flowetl/flowetl/operators/staging/sql"
)

with DAG(
    dag_id="load_records_from_staging_dag",
    start_date=datetime(2021, 9, 29),  # Put this back before pr
    end_date=datetime(2021, 9, 29),
    # Defined in the docker-config.yml
    default_args={"owner": "airflow", "postgres_conn_id": "flowdb"},
    params={
        "flowetl_csv_dir": os.getenv("FLOWETL_CSV_DIR"),
        "flowdb_csv_dir": os.getenv("FLOWDB_CSV_DIR"),
    },
    template_searchpath=template_folder,
) as dag:
    from flowetl.operators.staging.create_and_fill_day_sightings_table import (
        CreateAndFillDaySightingsTable,
    )
    from flowetl.operators.staging.create_sightings_table import CreateSightingsTable
    from flowetl.operators.staging.default_location_mapping import (
        DefaultLocationMapping,
    )
    from flowetl.operators.staging.append_sightings_to_main_table import (
        AppendSightingsToMainTable,
    )
    from flowetl.operators.staging.apply_mapping_to_staged_events import (
        ApplyMappingToStagedEvents,
    )
    from flowetl.operators.staging.create_and_fill_staging_table import (
        CreateAndFillStagingTable,
    )

    create_and_fill_staging_table = CreateAndFillStagingTable()
    location_mapping = DefaultLocationMapping()
    apply_mapping_to_staged_events = ApplyMappingToStagedEvents()
    create_sightings_table = CreateSightingsTable()
    create_day_sightings_table = CreateAndFillDaySightingsTable()
    append_sightings = AppendSightingsToMainTable()

    # Todo: Add cleanup step (CSVs and foreign tables)

    append_sightings << [create_day_sightings_table, create_sightings_table]
    (
        create_day_sightings_table
        << apply_mapping_to_staged_events
        << location_mapping
        << create_and_fill_staging_table
    )


with DAG(dag_id="test_dag", start_date=datetime(2016, 1, 1)) as dag_2:
    op = DummyOperator(task_id="foo")
