from etl.model import ETLRecord
from airflow.models import DagRun


def test_single_file_never_seen(
    write_files_to_dump, trigger_dags, flowetl_db_session, wait_for_completion
):

    write_files_to_dump(file_names=["CALLS_20160101.csv.gz", "SMS_20160101.csv.gz"])
    trigger_dags()
    wait_for_completion("success", "etl_calls", session=flowetl_db_session)
    wait_for_completion("success", "etl_sms", session=flowetl_db_session)
    assert False

