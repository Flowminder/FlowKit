# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Make sure that airflow is able to pick up the correct DAGs
"""
from pathlib import Path

import pytest

dag_folder = str(Path(__file__).parent.parent.parent / "mounts" / "dags")


@pytest.mark.usefixtures("airflow_local_setup")
def test_dags_present():
    """
    Test that the correct dags are parsed
    """
    from airflow.models import DagBag

    assert set(
        DagBag(
            dag_folder=dag_folder,
            include_examples=False,
        ).dag_ids
    ) == set(["remote_table_dag", "filesystem_dag", "test_dag"])


@pytest.mark.usefixtures("airflow_local_setup")
@pytest.mark.parametrize(
    "dag_name,expected_task_list",
    [
        (
            "remote_table_dag",
            {
                "add_constraints",
                "add_indexes",
                "analyze",
                "analyze_parent",
                "analyze_parent_only_for_new",
                "attach",
                "check_not_in_flux",
                "count_added_rows",
                "count_duplicated",
                "count_duplicates",
                "count_location_ids",
                "count_msisdns",
                "dumy_qa_check",
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
                "create_staging_view",
                "extract",
                "update_records",
                "update_location_ids_table",
                "wait_for_data",
            },
        ),
        (
            "filesystem_dag",
            {
                "add_constraints",
                "add_indexes",
                "analyze",
                "analyze_parent",
                "analyze_parent_only_for_new",
                "attach",
                "check_not_in_flux",
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
                "cluster",
                "create_staging_view",
                "extract",
                "update_records",
                "update_location_ids_table",
                "wait_for_data",
            },
        ),
        ("test_dag", {"flowetl_install_test_op", "flowdb_connect_test_op"}),
    ],
)
def test_correct_tasks(airflow_local_setup, dag_name, expected_task_list):
    """
    Test that each dag has the tasks expected
    """
    from airflow.models import DagBag

    dag = DagBag(
        dag_folder=dag_folder,
        include_examples=False,
    ).dags[dag_name]
    assert set(dag.task_ids) == expected_task_list
