from pendulum import parse

from etl.model import ETLRecord


# def test_single_file_never_seen(
#     flowetl_container,
#     write_files_to_dump,
#     trigger_dags,
#     flowetl_db_session,
#     wait_for_completion,
# ):

#     write_files_to_dump(file_names=["CALLS_20160101.csv.gz"])
#     trigger_dags(flowetl_container=flowetl_container)
#     wait_for_completion("success", "etl_calls", session=flowetl_db_session)


# def test_single_file_bad_name(
#     flowetl_container,
#     write_files_to_dump,
#     trigger_dags,
#     flowetl_db_session,
#     wait_for_completion,
# ):

#     write_files_to_dump(file_names=["bad_file.bad"])
#     trigger_dags(flowetl_container=flowetl_container)
#     wait_for_completion("success", "etl_sensor", session=flowetl_db_session)


# def test_multiple_files_different_type_never_seen(
#     flowetl_container,
#     write_files_to_dump,
#     trigger_dags,
#     flowetl_db_session,
#     wait_for_completion,
# ):

#     write_files_to_dump(
#         file_names=[
#             "CALLS_20160101.csv.gz",
#             "SMS_20160101.csv.gz",
#             "MDS_20160101.csv.gz",
#             "TOPUPS_20160101.csv.gz",
#         ]
#     )
#     trigger_dags(flowetl_container=flowetl_container)
#     wait_for_completion("success", "etl_calls", session=flowetl_db_session)
#     wait_for_completion("success", "etl_sms", session=flowetl_db_session)
#     wait_for_completion("success", "etl_mds", session=flowetl_db_session)
#     wait_for_completion("success", "etl_topups", session=flowetl_db_session)


# def test_multiple_files_same_type_never_seen(
#     flowetl_container,
#     write_files_to_dump,
#     trigger_dags,
#     flowetl_db_session,
#     wait_for_completion,
# ):

#     write_files_to_dump(
#         file_names=[
#             "CALLS_20160101.csv.gz",
#             "CALLS_20160102.csv.gz",
#             "CALLS_20160103.csv.gz",
#             "CALLS_20160104.csv.gz",
#         ]
#     )
#     trigger_dags(flowetl_container=flowetl_container)
#     wait_for_completion("success", "etl_calls", count=4, session=flowetl_db_session)


def test_single_file_previously_quarantined(
    flowetl_container,
    write_files_to_dump,
    trigger_dags,
    flowetl_db_session,
    flowdb_session,
    flowdb_connection,
    wait_for_completion,
):
    write_files_to_dump(file_names=["CALLS_20160101.csv.gz"])
    trigger_dags(flowetl_container=flowetl_container)
    wait_for_completion("success", "etl_calls", session=flowetl_db_session)

    # Simulation of failed ingestion - change state in etl.etl_records...
    file_data = {
        "cdr_type": "calls",
        "cdr_date": parse("2016-01-01").date(),
        "state": "quarantine",
    }
    ETLRecord.set_state(
        cdr_type=file_data["cdr_type"],
        cdr_date=file_data["cdr_date"],
        state=file_data["state"],
        session=flowdb_session,
    )
    # ...and drop the events table
    connection, trans = flowdb_connection
    connection.execute("DROP TABLE events.calls_20160101")
    trans.commit()

    write_files_to_dump(file_names=["CALLS_20160101.csv.gz"])
    trigger_dags(flowetl_container=flowetl_container)
    wait_for_completion("success", "etl_calls", count=2, session=flowetl_db_session)


# def test_single_file_previously_archived(
#     flowetl_container,
#     write_files_to_dump,
#     trigger_dags,
#     flowetl_db_session,
#     flowdb_session,
#     wait_for_completion,
# ):

#     write_files_to_dump(file_names=["CALLS_20160101.csv.gz"])
#     trigger_dags(flowetl_container=flowetl_container)
#     wait_for_completion("success", "etl_calls", session=flowetl_db_session)

#     write_files_to_dump(file_names=["CALLS_20160101.csv.gz"])
#     trigger_dags(flowetl_container=flowetl_container)
#     wait_for_completion("success", "etl_sensor", count=2, session=flowetl_db_session)
