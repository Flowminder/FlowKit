# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from time import sleep

import pandas as pd


def test_file_pipeline(
    run_dag,
    dag_status,
    task_status,
    flowdb_transaction,
):
    """
    Test full run of file pipeline, and ensure that:

    1. Data is there
    2. Table is proper subtable
    3. Table is correctly named
    4. QA checks are run
    5. ETL metadata is recorded
    6. Table is indexed
    7. Table is clustered
    8. Table is date constrained
    9. All location IDs today are in events.location_ids table

    """
    exit_code, output = run_dag(dag_id="filesystem_dag", exec_date="2016-03-01")
    assert exit_code == 0
    for check_attempt in range(100):  # Wait for dag to stop running
        exit_code, status = dag_status(dag_id="filesystem_dag", exec_date="2016-03-01")
        if b"running" not in status:
            break
        sleep(5)
    assert b"success" in status

    # Check data present
    date_present = flowdb_transaction.execute(
        "SELECT count(*) FROM events.calls WHERE datetime::date = '2016-03-01';"
    ).fetchall()
    assert date_present[0][0] > 0

    date_present = flowdb_transaction.execute(
        "SELECT count(*) FROM events.calls_20160301;"
    ).fetchall()
    assert date_present[0][0] > 0

    # Check table is inherited

    exists_query = f"""SELECT EXISTS(SELECT relname 
        FROM 
            pg_inherits i 
        JOIN 
            pg_class c 
        ON 
            c.oid = inhrelid 
        WHERE 
            inhparent = 'events.calls'::regclass
        AND
            relname = 'calls_20160301')"""
    assert flowdb_transaction.execute(exists_query).fetchall()[0][0]

    # Check table is clustered on the right field

    clustered_query = f"""SELECT EXISTS(
    SELECT
        i.relname
    FROM
        pg_index AS idx
    JOIN
        pg_class AS i
    ON
        i.oid = idx.indexrelid
    WHERE
        idx.indisclustered
    AND 
        idx.indrelid::regclass = 'events.calls_20160301'::regclass
    AND
        i.relname = 'calls_20160301_msisdn_idx')
    """
    assert flowdb_transaction.execute(clustered_query).fetchall()[0][0]

    # Check table has date constraints

    constraint_query = f"""SELECT 
        pg_get_constraintdef(c.oid)
    FROM   
        pg_constraint c
    JOIN   
        pg_namespace n 
    ON 
        n.oid = c.connamespace
    WHERE  
        contype ='c' 
    AND 
        conrelid::regclass = 'events.calls_20160301'::regclass
    """
    constraint_string = f"CHECK (((datetime >= '2016-03-01 00:00:00+00'::timestamp with time zone) AND (datetime < '2016-03-02 00:00:00+00'::timestamp with time zone)))"
    assert (
        flowdb_transaction.execute(constraint_query).fetchall()[0][0].replace("\n", "")
        == constraint_string
    )

    # Check ETL meta

    etl_meta_query = "SELECT EXISTS(SELECT * FROM etl.etl_records WHERE cdr_date='2016-03-01' AND state='ingested' and cdr_type='calls');"
    assert flowdb_transaction.execute(etl_meta_query).fetchall()[0][0]

    # Check all location IDs today are in events.location_ids table

    location_ids_query = """
    SELECT NOT EXISTS (
        SELECT location_id
        FROM events.calls_20160301
        LEFT JOIN (
            SELECT location_id
            FROM events.location_ids
            WHERE cdr_type = 'calls'
            AND '2016-03-01'::date BETWEEN first_active_date AND last_active_date
        ) active_location_ids
        USING (location_id)
        WHERE active_location_ids.location_id IS NULL
    )
    """
    assert flowdb_transaction.execute(location_ids_query).fetchall()[0][0]

    # Check qa checks

    qa_check_query = "SELECT type_of_query_or_check from etl.post_etl_queries WHERE cdr_date='2016-03-01' AND cdr_type='calls'"
    assert sorted(
        row[0] for row in flowdb_transaction.execute(qa_check_query).fetchall()
    ) == sorted(
        [
            "count_added_rows",
            "count_duplicated",
            "count_duplicates",
            "count_location_ids",
            "count_msisdns",
            "dummy_qa_check",
            "earliest_timestamp",
            "latest_timestamp",
            "count_imeis",
            "count_imsis",
            "count_locatable_events",
            "count_locatable_location_ids",
            "count_null_imeis",
            "count_null_imsis",
            "count_null_location_ids",
            "max_msisdns_per_imei",
            "max_msisdns_per_imsi",
            "count_added_rows_outgoing.calls",
            "count_null_counterparts.calls",
            "count_null_durations.calls",
            "count_onnet_msisdns_incoming.calls",
            "count_onnet_msisdns_outgoing.calls",
            "count_onnet_msisdns.calls",
            "max_duration.calls",
            "median_duration.calls",
        ]
    )


def test_file_pipeline_bad_file(
    run_dag,
    dag_status,
    task_status,
    flowdb_transaction,
):
    """
    Test fail for bad data file.
    """
    exit_code, output = run_dag(dag_id="filesystem_dag", exec_date="2016-03-02")
    assert exit_code == 0
    for check_attempt in range(100):  # Wait for dag to stop running
        exit_code, status = dag_status(dag_id="filesystem_dag", exec_date="2016-03-02")
        if b"running" not in status:
            break
        sleep(5)
    assert b"failed" in status
    date_present = flowdb_transaction.execute(
        "SELECT count(*) FROM events.calls WHERE datetime::date = '2016-03-02';"
    ).fetchall()
    assert date_present[0][0] == 0


def test_get_only_one_day(populated_test_data_table, run_task, all_tasks):
    """
    Test that only data for the one day is returned even if other data is present.
    """
    for task_id in all_tasks:
        return_code, result = run_task(
            dag_id="remote_table_dag", task_id=task_id, exec_date="2016-06-15"
        )
        print(
            f"Dag: remote_table_dag, task: {task_id}, exec date: 2016-06-15.\n\n{result}\n\n"
        )
    db_content = pd.read_sql_table(
        "calls_20160615", populated_test_data_table, "events"
    )
    assert len(db_content) == 1


def test_wait_when_in_flux(growing_test_data_table, run_task):
    """
    Test the table flux sensor waits if the table is still being written to.
    """
    for task_id in ["create_staging_view", "wait_for_data"]:
        return_code, result = run_task(
            dag_id="remote_table_dag", task_id=task_id, exec_date="2016-06-15"
        )
        print(
            f"Dag: remote_table_dag, task: {task_id}, exec date: 2016-06-15.\n\n{result}\n\n"
        )

    return_code, result = run_task(
        dag_id="remote_table_dag", task_id="check_not_in_flux", exec_date="2016-06-15"
    )
    print(
        f"Dag: remote_table_dag, task: check_not_in_flux, exec date: 2016-06-15.\n\n{result}\n\n"
    )
    assert "Success criteria met. Exiting." not in str(result)
    assert return_code == 0
