def test_csv_to_sightings(run_dag):
    run_dag(
        dag_id="append_sightings_to_main_table",
    )
