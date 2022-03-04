from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.bash_operator import BashOperator
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
    sms_operator = PostgresOperator(
        sql="mount_event.sql",
        task_id="mount_event",
        params={"event_type": "sms"},
        dag=mock_staging_dag,
    )
    run_task(sms_operator, mock_staging_dag)
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
    fill_operator = PostgresOperator(
        sql="stage_events.sql",
        task_id="stage_events",
        params={"event_types": ["call", "sms"]},
        dag=mock_staging_dag,
    )
    run_task(
        fill_operator,
        mock_staging_dag,
    )
    out = mounted_events_conn.execute("SELECT * FROM staging_table_20210929")
    assert out.rowcount == 20


def test_append_sightings_to_main_table(mock_staging_dag, day_sightings_table_conn):
    # Needs sightings table to exist
    append_operator = PostgresOperator(
        sql="append_sightings_to_main_table.sql",
        task_id="append_sightings_to_main_table",
        dag=mock_staging_dag,
    )
    run_task(append_operator, mock_staging_dag)
    out = day_sightings_table_conn.execute("SELECT * FROM reduced.sightings")
    assert out.rowcount == 37


def test_create_and_fill_day_sightings_table(mock_staging_dag, sightings_table_conn):
    # Needs staging table + reduced sightings
    day_sightings = PostgresOperator(
        sql="create_and_fill_day_sightings_table.sql",
        task_id="create_and_fill_day_sightings_table",
        dag=mock_staging_dag,
    )
    run_task(day_sightings, mock_staging_dag)
    out = sightings_table_conn.execute("SELECT * FROM reduced.sightings_20210929")
    assert out.rowcount == 37  # two merged


def test_create_sightings_table(mock_staging_dag, dummy_db_conn):
    create_sightings = PostgresOperator(
        sql="create_sightings_table.sql",
        task_id="create_sightings_table",
        dag=mock_staging_dag,
    )
    run_task(create_sightings, mock_staging_dag)
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

    cleanup = PostgresOperator(
        sql="cleanup_staging_table.sql",
        task_id="cleanup_staging_table",
        dag=mock_staging_dag,
    )
    run_task(cleanup, mock_staging_dag)

    # Check staging tables have been cleared up
    table_list = staged_data_conn.execute(
        f"""
        SELECT table_name
        FROM information_schema.tables
        """
    ).fetchall()
    after_table_names = [row["table_name"] for row in table_list]
    assert all(st not in after_table_names for st in staging_tables)
