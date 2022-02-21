from datetime import datetime
from time import sleep
from pathlib import Path
from pprint import pprint


def test_csv_to_sightings(flowetl_container, run_dag, dag_status, flowdb_transaction):
    test_date = "20210929"
    foo, bar = flowetl_container.exec_run("airflow dags report")
    print(bar.decode())
    # exit_code, output = run_dag(
    #     dag_id="load_records_from_staging_dag",
    #     exec_date=test_date
    # )
    # print(output.decode('utf-8'))
    _, config = flowetl_container.exec_run("airflow config")
    print(config.decode())
    exit_code, unpause_out = flowetl_container.exec_run(
        "airflow dags unpause load_records_from_staging_dag"
    )
    print(unpause_out.decode())
    assert exit_code == 0
    for check_attempt in range(100):
        exit_code, status = dag_status(
            dag_id="load_records_from_staging_dag", exec_date=test_date
        )
        print(status.decode())
        if b"running" not in status:
            break
        sleep(1)
    exit_code, logs = flowetl_container.exec_run(
        "airflow dags list-runs -d load_records_from_staging_dag"
    )
    # assert b"success" in status

    # Check staging table was created properly
    staged_record_count = flowdb_transaction.execute(
        f"SELECT count(*) FROM staging_table_{test_date};"
    ).fetchall()[0][
        0
    ]  # gotta be a better way
    print(f"Staged records: {staged_record_count}")
    assert staged_record_count == 39

    # Check correct data is in reduced table
    record_count = flowdb_transaction.execute(
        f"SELECT count(*) FROM reduced.sightings WHERE sighting_date = date '{test_date}'"
    ).fetchall()[0][0]
    assert record_count == 37

    # Check partition is correctly attached
    # From https://dba.stackexchange.com/questions/40441/get-all-partition-names-for-a-table
    partition_list = flowdb_transaction.execute(
        f"""
        SELECT inhrelid::regclass AS child
        FROM   pg_catalog.pg_inherits
        WHERE  inhparent = 'reduced.sightings'::regclass;
        """
    ).fetchall()
    assert ("reduced.sightings_20210929",) in partition_list

    # Check staging files have been cleared up
