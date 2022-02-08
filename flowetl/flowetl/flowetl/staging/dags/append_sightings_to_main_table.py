from airflow import DAG

# TODO Tomorrow: Design the ingestion DAG


def csv_ingestion_dag(csvs, date):
    with DAG(
        "append_sightings_to_main_table",
        params={"csv_dir": csvs, "date": date},
    ) as dag:
        from ..operators.create_and_fill_day_sightings_table import (
            CreateAndFillDaySightingsTable,
        )
        from ..operators.create_sightings_table import CreateSightingsTable
        from ..operators.default_location_mapping import DefaultLocationMapping
        from ..operators.append_sightings_to_main_table import (
            AppendSightingsToMainTable,
        )
        from ..operators.apply_mapping_to_staged_events import (
            ApplyMappingToStagedEvents,
        )
        from ..operators.create_and_fill_staging_table import CreateAndFillStagingTable

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
        return dag
