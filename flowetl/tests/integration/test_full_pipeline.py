import os

from pathlib import Path
from pendulum import parse

from etl.model import ETLRecord


def test_single_file_previously_quarantined(
    flowetl_container,
    write_files_to_dump,
    trigger_dags,
    wait_for_completion,
    flowetl_db_session,
    flowdb_session,
):
    write_files_to_dump(
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
    CALLS_20160101_record = {
        "cdr_type": "calls",
        "cdr_date": parse("2016-01-01").date(),
        "state": "archive",
    }
    ETLRecord.set_state(
        cdr_type=CALLS_20160101_record["cdr_type"],
        cdr_date=CALLS_20160101_record["cdr_date"],
        state=CALLS_20160101_record["state"],
        session=flowdb_session,
    )

    SMS_20160101_record = {
        "cdr_type": "sms",
        "cdr_date": parse("2016-01-01").date(),
        "state": "quarantine",
    }
    ETLRecord.set_state(
        cdr_type=SMS_20160101_record["cdr_type"],
        cdr_date=SMS_20160101_record["cdr_date"],
        state=SMS_20160101_record["state"],
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

    dump_files = ["CALLS_20160101.csv.gz", "bad_file.bad"]  # should have been ignored
    archive_files = [
        "CALLS_20160102.csv.gz",
        "SMS_20160101.csv.gz",
        "MDS_20160101.csv.gz",
        "TOPUPS_20160101.csv.gz",
    ]  # ingested so now in archive

    dump = [file.name for file in Path(f"{os.getcwd()}/mounts/dump").glob("*")]
    archive = [file.name for file in Path(f"{os.getcwd()}/mounts/archive").glob("*")]
    quarantine = [
        file.name for file in Path(f"{os.getcwd()}/mounts/quarantine").glob("*")
    ]
    ingest = [file.name for file in Path(f"{os.getcwd()}/mounts/ingest").glob("*")]

    assert set(dump_files) == (set(dump) - set(["README.md"]))
    assert set(archive_files) == (set(archive) - set(["README.md"]))

    # quarantine and ingest should be empty
    assert set() == (set(quarantine) - set(["README.md"]))
    assert set() == (set(ingest) - set(["README.md"]))

    # make sure tables expected exist in flowdb

    # make sure etl_records table is what we expect

