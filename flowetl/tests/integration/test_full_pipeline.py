import os

from pathlib import Path
from pendulum import parse

from etl.model import ETLRecord


def test_single_file_previously_quarantined(
    flowetl_container,
    write_files_to_files,
    trigger_dags,
    wait_for_completion,
    flowetl_db_session,
    flowdb_session,
    flowdb_connection,
):
    """
    Test for full pipeline. We want to test the following things;

    1. Do files in the files location get picked up?
    2. Do files that do not match a configuration pattern get ignored?
    3. Do files (cdr_type, cdr_date pairs) that have a state of archive
    in etl.etl_records get ignored?
    4. Do files (cdr_type, cdr_date pairs) that have a state of quarantine
    in etl.etl_records get picked up to be reprocessed?
    5. Do files of different CDR types cause the correct etl_{cdr_type}
    DAG to run?
    6. Do child tables get created under the associated parent table in
    the events schema?
    """
    write_files_to_files(
        file_names=[
            "CALLS_20160101.csv.gz",
            "CALLS_20160102.csv.gz",
            "SMS_20160101.csv.gz",
            "bad_file.bad",
            "MDS_20160101.csv.gz",
            "TOPUPS_20160101.csv.gz",
        ]
    )

    # set CALLS_20160101 as archived and SMS_20160101 as quarantined
    ETLRecord.set_state(
        cdr_type="calls",
        cdr_date=parse("2016-01-01").date(),
        state="archive",
        session=flowdb_session,
    )
    ETLRecord.set_state(
        cdr_type="sms",
        cdr_date=parse("2016-01-01").date(),
        state="quarantine",
        session=flowdb_session,
    )

    trigger_dags(flowetl_container=flowetl_container)

    # 1 calls, 1 sms, 1 mds and 1 topups DAG should run and we wait for
    # their completion
    wait_for_completion("success", "etl_calls", session=flowetl_db_session)
    wait_for_completion("success", "etl_sms", session=flowetl_db_session)
    wait_for_completion("success", "etl_mds", session=flowetl_db_session)
    wait_for_completion("success", "etl_topups", session=flowetl_db_session)

    # make sure files are where they should be

    all_files = [
        "CALLS_20160101.csv.gz",
        "bad_file.bad",
        "CALLS_20160102.csv.gz",
        "SMS_20160101.csv.gz",
        "MDS_20160101.csv.gz",
        "TOPUPS_20160101.csv.gz",
    ]  # all files

    files = [file.name for file in Path(f"{os.getcwd()}/mounts/files").glob("*")]

    assert set(all_files) == (set(files) - set(["README.md"]))

    def expected_table_exists(*, table_name):
        """
        Return True if the expected table exists in flowdb, otherwise False.
        """
        connection, _ = flowdb_connection
        sql = """
        select
            count(*)
        from
            information_schema.tables
        where
            table_schema = 'events'
            and
            table_name = '{table_name}'
        """
        res = connection.execute(sql.format(table_name=table_name)).fetchone()[0]
        return res == 1

    assert expected_table_exists(table_name="calls_20160102")
    assert expected_table_exists(table_name="sms_20160101")
    assert expected_table_exists(table_name="mds_20160101")
    assert expected_table_exists(table_name="topups_20160101")

    def get_etl_states(*, cdr_type, cdr_date):
        """
        Return the ETL states present for the given cdr type and date.
        """
        res = (
            flowdb_session.query(ETLRecord.state)
            .filter(ETLRecord.cdr_type == cdr_type, ETLRecord.cdr_date == cdr_date)
            .all()
        )
        return sorted([row[0] for row in res])

    # calls,20160101 -> archive
    etl_states = set(get_etl_states(cdr_type="calls", cdr_date="2016-01-01"))
    etl_states_expected = set(["archive"])
    assert etl_states_expected == etl_states

    # calls,20160102 -> ingest + archive
    etl_states = get_etl_states(cdr_type="calls", cdr_date="2016-01-02")
    etl_states_expected = ["archive", "ingest"]
    assert etl_states_expected == etl_states

    # sms,20160101 -> quarantine + ingest + archive
    etl_states = get_etl_states(cdr_type="sms", cdr_date="2016-01-01")
    etl_states_expected = ["archive", "ingest", "quarantine"]
    assert etl_states_expected == etl_states

    # mds,20160101 -> ingest + archive
    etl_states = get_etl_states(cdr_type="mds", cdr_date="2016-01-01")
    etl_states_expected = ["archive", "ingest"]
    assert etl_states_expected == etl_states

    # topups,20160101 -> ingest + archive
    etl_states = get_etl_states(cdr_type="topups", cdr_date="2016-01-01")
    etl_states_expected = ["archive", "ingest"]
    assert etl_states_expected == etl_states
