# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest

from marshmallow import ValidationError
from prefect import Flow, Parameter
from prefect.environments import storage
from prefect.schedules import Schedule

from autoflow.parser.available_dates_sensor_schema import AvailableDatesSensorSchema
from autoflow.sensor import WorkflowConfig


def test_available_dates_sensor_schema(tmpdir):
    """
    Test that AvailableDatesSensorSchema can load a valid set of sensor config parameters.
    """
    # Create storage object to pass in context
    dummy_workflow = Flow(name="DUMMY_WORKFLOW")
    for parameter in ["reference_date", "date_ranges", "DUMMY_PARAM"]:
        dummy_workflow.add_task(Parameter(parameter))
    workflow_storage = storage.Local(tmpdir)
    workflow_storage.add_flow(dummy_workflow)

    input_dict = dict(
        schedule="0 0 * * *",
        cdr_types=["calls", "sms", "mds", "topups"],
        workflows=[
            {"workflow_name": "DUMMY_WORKFLOW", "parameters": {"DUMMY_PARAM": 1}},
            {"workflow_name": "DUMMY_WORKFLOW", "parameters": {"DUMMY_PARAM": 2}},
        ],
    )
    sensor_config = AvailableDatesSensorSchema(
        context={"workflow_storage": workflow_storage}
    ).load(input_dict)
    assert isinstance(sensor_config["schedule"], Schedule)
    assert sensor_config["schedule"].clocks[0].cron == "0 0 * * *"
    assert sensor_config["cdr_types"] == ["calls", "sms", "mds", "topups"]
    assert sensor_config["workflows"] == [
        WorkflowConfig(**workflow) for workflow in input_dict["workflows"]
    ]


def test_available_dates_sensor_schema_null_schedule():
    """
    Test that AvailableDatesSensorSchema can load schedule=None.
    """
    input_dict = dict(schedule=None, cdr_types=["calls"], workflows=[])
    sensor_config = AvailableDatesSensorSchema().load(input_dict)
    assert sensor_config["schedule"] is None
    assert sensor_config["cdr_types"] == ["calls"]
    assert sensor_config["workflows"] == []


def test_available_dates_sensor_schema_missing_schedule():
    """
    Test that AvailableDatesSensorSchema raises a ValidationError if the
    'schedule' field is missing.
    """
    input_dict = dict(cdr_types=["calls"], workflows=[])
    with pytest.raises(ValidationError) as exc_info:
        sensor_config = AvailableDatesSensorSchema().load(input_dict)
    assert "Missing data for required field." in exc_info.value.messages["schedule"]


def test_available_dates_sensor_schema_missing_cdr_types():
    """
    Test that AvailableDatesSensorSchema 'cdr_types' field loads None if the
    input 'cdr_types' field is missing.
    """
    input_dict = dict(schedule="0 0 * * *", workflows=[])
    sensor_config = AvailableDatesSensorSchema().load(input_dict)
    assert sensor_config["cdr_types"] is None


def test_available_dates_sensor_schema_invalid_cdr_types():
    """
    Test that AvailableDatesSensorSchema raises a ValidationError if the
    'cdr_types' field contains invalid values.
    """
    input_dict = dict(
        schedule="0 0 * * *", cdr_types=["calls", "NOT_A_CDR_TYPE"], workflows=[]
    )
    with pytest.raises(ValidationError) as exc_info:
        sensor_config = AvailableDatesSensorSchema().load(input_dict)
    assert (
        "Must be one of: calls, sms, mds, topups."
        in exc_info.value.messages["cdr_types"][1]
    )


def test_available_dates_sensor_schema_missing_workflows():
    """
    Test that AvailableDatesSensorSchema raises a ValidationError if the
    'workflows' field is missing.
    """
    input_dict = dict(schedule="0 0 * * *", cdr_types=["calls"])
    with pytest.raises(ValidationError) as exc_info:
        sensor_config = AvailableDatesSensorSchema().load(input_dict)
    assert "Missing data for required field." in exc_info.value.messages["workflows"]
