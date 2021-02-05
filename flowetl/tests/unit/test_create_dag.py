# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from datetime import datetime

import pytest
from flowetl.util import create_dag


@pytest.mark.parametrize(
    "bad_config", [dict(), dict(staging_view_sql=""), dict(filename="", fields={})]
)
def test_viable_source_provided_error(bad_config):
    with pytest.raises(
        TypeError,
        match="Either staging_view_sql and source_table, or filename and fields must be provided.",
    ):
        create_dag(
            dag_id="TEST",
            cdr_type="TEST",
            start_date=datetime.now(),
            extract_sql="DUMMY SQL",
            **bad_config
        )


def test_source_table_macro_added():
    dag = create_dag(
        dag_id="TEST",
        cdr_type="TEST",
        start_date=datetime.now(),
        extract_sql="DUMMY SQL",
        staging_view_sql="DUMMY STAGING SQL",
        source_table="DUMMY_SOURCE_TABLE",
    )
    assert dag.user_defined_macros["source_table"] == "DUMMY_SOURCE_TABLE"


@pytest.mark.parametrize(
    "args, expected_view_type, expected_extract_type, expected_flux_sensor_type",
    [
        (
            dict(
                staging_view_sql="DUMMY STAGING SQL", source_table="DUMMY_SOURCE_TABLE"
            ),
            "CreateStagingViewOperator",
            "ExtractFromViewOperator",
            "TableFluxSensor",
        ),
        (
            dict(filename="DUMMY FILE PATTERN", fields=dict(DUMMY_FIELD="DUMMY_TYPE")),
            "CreateForeignStagingTableOperator",
            "ExtractFromForeignTableOperator",
            "FileFluxSensor",
        ),
    ],
)
def test_inferred_op_types(
    args, expected_view_type, expected_extract_type, expected_flux_sensor_type
):
    dag = create_dag(
        dag_id="TEST",
        cdr_type="TEST",
        start_date=datetime.now(),
        extract_sql="DUMMY SQL",
        **args
    )
    assert dag.task_dict["create_staging_view"].__class__.__name__ == expected_view_type
    assert dag.task_dict["extract"].__class__.__name__ == expected_extract_type
    assert (
        dag.task_dict["check_not_in_flux"].__class__.__name__
        == expected_flux_sensor_type
    )


def test_no_cluster_by_default():
    dag = create_dag(
        dag_id="TEST",
        cdr_type="TEST",
        start_date=datetime.now(),
        extract_sql="DUMMY SQL",
        staging_view_sql="DUMMY STAGING SQL",
        source_table="DUMMY_SOURCE_TABLE",
    )
    assert "cluster" not in dag.task_dict


def test_cluster_set_when_field_given():
    dag = create_dag(
        dag_id="TEST",
        cdr_type="TEST",
        start_date=datetime.now(),
        extract_sql="DUMMY SQL",
        staging_view_sql="DUMMY STAGING SQL",
        source_table="DUMMY_SOURCE_TABLE",
        cluster_field="DUMMY_FIELD",
    )
    assert "cluster" in dag.task_dict


@pytest.mark.parametrize(
    "args",
    [
        dict(staging_view_sql="DUMMY STAGING SQL", source_table="DUMMY_SOURCE_TABLE"),
        dict(filename="DUMMY FILE PATTERN", fields=dict(DUMMY_FIELD="DUMMY_TYPE")),
    ],
)
def test_flux_check_can_be_disabled(args):
    """
    Flux check task is not present when use_flux_sensor=True
    """
    dag = create_dag(
        dag_id="TEST",
        cdr_type="TEST",
        start_date=datetime.now(),
        extract_sql="DUMMY SQL",
        use_flux_sensor=False,
        **args
    )
    assert "check_not_in_flux" not in dag.task_dict


@pytest.mark.parametrize(
    "use_flux_sensor, expected_flux_sensor_type",
    [("file", "FileFluxSensor"), ("table", "TableFluxSensor")],
)
def test_choose_flux_sensor_type(use_flux_sensor, expected_flux_sensor_type):
    """
    Type of flux check is set according to the 'use_flux_check' argument
    """
    dag = create_dag(
        dag_id="TEST",
        cdr_type="TEST",
        start_date=datetime.now(),
        extract_sql="DUMMY SQL",
        filename="DUMMY FILE PATTERN",
        fields=dict(DUMMY_FIELD="DUMMY_TYPE"),
        use_flux_sensor=use_flux_sensor,
    )
    assert (
        dag.task_dict["check_not_in_flux"].__class__.__name__
        == expected_flux_sensor_type
    )


def test_invalid_flux_sensor_error():
    """
    Attempting to use file flux sensor when not extracting from a file raises an error
    """
    with pytest.raises(
        ValueError, match="File flux sensor can only be used when loading from a file.",
    ):
        dag = create_dag(
            dag_id="TEST",
            cdr_type="TEST",
            start_date=datetime.now(),
            extract_sql="DUMMY SQL",
            staging_view_sql="DUMMY STAGING SQL",
            source_table="DUMMY_SOURCE_TABLE",
            use_flux_sensor="file",
        )


def test_use_file_flux_sensor_deprecated():
    """
    Setting 'use_file_flux_sensor=False' produces deprecation warning and uses TableFluxSensor
    """
    with pytest.deprecated_call(
        match="The 'use_file_flux_sensor' argument is deprecated. Set use_flux_sensor='table' instead."
    ):
        dag = create_dag(
            dag_id="TEST",
            cdr_type="TEST",
            start_date=datetime.now(),
            extract_sql="DUMMY SQL",
            filename="DUMMY FILE PATTERN",
            fields=dict(DUMMY_FIELD="DUMMY_TYPE"),
            use_file_flux_sensor=False,
        )
    assert dag.task_dict["check_not_in_flux"].__class__.__name__ == "TableFluxSensor"
