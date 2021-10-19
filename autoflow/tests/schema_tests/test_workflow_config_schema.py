# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest

import datetime

from marshmallow import ValidationError
from prefect import Flow, Parameter
from prefect import storage

from autoflow.date_stencil import DateStencil
from autoflow.parser.workflow_config_schema import WorkflowConfigSchema
from autoflow.sensor import WorkflowConfig


def test_workflow_config_schema(tmpdir):
    """
    Test that WorkflowConfigSchema loads input data into a WorkflowConfig namedtuple.
    """
    # Create storage object to pass in context
    dummy_workflow = Flow(name="DUMMY_WORKFLOW")
    for parameter in ["reference_date", "date_ranges", "DUMMY_PARAM"]:
        dummy_workflow.add_task(Parameter(parameter))
    workflow_storage = storage.Local(tmpdir)
    workflow_storage.add_flow(dummy_workflow)

    input_dict = dict(
        workflow_name="DUMMY_WORKFLOW",
        parameters={"DUMMY_PARAM": "DUMMY_VALUE"},
        earliest_date=datetime.date(2016, 1, 1),
        date_stencil=[-1, 0],
    )
    workflow_config = WorkflowConfigSchema(
        context={"workflow_storage": workflow_storage}
    ).load(input_dict)
    assert isinstance(workflow_config, WorkflowConfig)
    assert workflow_config.workflow_name == input_dict["workflow_name"]
    assert workflow_config.parameters == input_dict["parameters"]
    assert workflow_config.earliest_date == input_dict["earliest_date"]
    assert workflow_config.date_stencil == DateStencil(input_dict["date_stencil"])


def test_workflow_config_schema_defaults(tmpdir):
    """
    Test that WorkflowConfigSchema loads input data if 'parameters',
    'earliest_date' and 'date_stencil' are not specified.
    """
    # Create storage object to pass in context
    dummy_workflow = Flow(name="DUMMY_WORKFLOW")
    for parameter in ["reference_date", "date_ranges"]:
        dummy_workflow.add_task(Parameter(parameter))
    workflow_storage = storage.Local(tmpdir)
    workflow_storage.add_flow(dummy_workflow)

    input_dict = dict(workflow_name="DUMMY_WORKFLOW")
    workflow_config = WorkflowConfigSchema(
        context={"workflow_storage": workflow_storage}
    ).load(input_dict)
    assert isinstance(workflow_config, WorkflowConfig)
    assert workflow_config == WorkflowConfig(workflow_name="DUMMY_WORKFLOW")


def test_workflow_config_schema_missing_workflow_name():
    """
    Test that WorkflowConfigSchema raises a ValidationError if 'workflow_name'
    is not provided.
    """
    input_dict = dict(
        parameters={"DUMMY_PARAM": "DUMMY_VALUE"},
        earliest_date=datetime.date(2016, 1, 1),
        date_stencil=[-1, 0],
    )
    with pytest.raises(ValidationError) as exc_info:
        workflow_config = WorkflowConfigSchema().load(input_dict)
    assert (
        "Missing data for required field." in exc_info.value.messages["workflow_name"]
    )


def test_workflow_config_schema_invalid_workflow_name():
    """
    Test that WorkflowConfigSchema raises a ValidationError if 'workflow_name'
    is not a string.
    """
    input_dict = dict(workflow_name=123)
    with pytest.raises(ValidationError) as exc_info:
        workflow_config = WorkflowConfigSchema().load({"workflow_name": 123})
    assert "Not a valid string." in exc_info.value.messages["workflow_name"]


@pytest.mark.parametrize("key", ["reference_date", "date_ranges"])
def test_workflow_config_schema_invalid_parameter_names(key, tmpdir):
    """
    Test that WorkflowConfigSchema raises a ValidationError if the 'parameters'
    dict keys contain 'reference_date' or 'date_ranges'.
    """
    # Create storage object to pass in context
    dummy_workflow = Flow(name="DUMMY_WORKFLOW")
    for parameter in ["reference_date", "date_ranges"]:
        dummy_workflow.add_task(Parameter(parameter))
    workflow_storage = storage.Local(tmpdir)
    workflow_storage.add_flow(dummy_workflow)

    input_dict = dict(workflow_name="DUMMY_WORKFLOW", parameters={key: "DUMMY_VALUE"})
    with pytest.raises(ValidationError) as exc_info:
        workflow_config = WorkflowConfigSchema(
            context={"workflow_storage": workflow_storage}
        ).load(input_dict)
    assert "Invalid input." in exc_info.value.messages["parameters"][key]["key"]


def test_workflow_config_schema_invalid_earliest_date(tmpdir):
    """
    Test that WorkflowConfigSchema raises a ValidationError if the
    'earliest_date' field is not a date.
    """
    # Create storage object to pass in context
    dummy_workflow = Flow(name="DUMMY_WORKFLOW")
    for parameter in ["reference_date", "date_ranges"]:
        dummy_workflow.add_task(Parameter(parameter))
    workflow_storage = storage.Local(tmpdir)
    workflow_storage.add_flow(dummy_workflow)

    input_dict = dict(workflow_name="DUMMY_WORKFLOW", earliest_date=datetime.time(11))
    with pytest.raises(ValidationError) as exc_info:
        workflow_config = WorkflowConfigSchema(
            context={"workflow_storage": workflow_storage}
        ).load(input_dict)
    assert "Not a valid date." in exc_info.value.messages["earliest_date"]


def test_workflow_config_schema_workflow_not_found(tmpdir):
    """
    Test that WorkflowConfigSchema raises a ValidationError if the named
    workflow does not exist.
    """
    # Create storage object to pass in context
    dummy_workflow = Flow(name="DUMMY_WORKFLOW")
    for parameter in ["reference_date", "date_ranges"]:
        dummy_workflow.add_task(Parameter(parameter))
    workflow_storage = storage.Local(tmpdir)
    workflow_storage.add_flow(dummy_workflow)

    with pytest.raises(ValidationError) as exc_info:
        workflow_config = WorkflowConfigSchema(
            context={"workflow_storage": workflow_storage}
        ).load({"workflow_name": "NONEXISTENT_WORKFLOW"})
    assert (
        "Workflow does not exist in this storage."
        in exc_info.value.messages["workflow_name"]
    )


@pytest.mark.parametrize("missing_parameter", ["reference_date", "date_ranges"])
def test_workflow_config_schema_workflow_does_not_accept_automatic_parameters(
    missing_parameter, tmpdir
):
    """
    Test that WorkflowConfigSchema raises a ValidationError if the named
    workflow does not accept parameters 'reference_date' and 'date_ranges'.
    """
    # Create storage object to pass in context
    dummy_workflow = Flow(name="DUMMY_WORKFLOW")
    for parameter in {"reference_date", "date_ranges"} - {missing_parameter}:
        dummy_workflow.add_task(Parameter(parameter))
    workflow_storage = storage.Local(tmpdir)
    workflow_storage.add_flow(dummy_workflow)

    with pytest.raises(ValidationError) as exc_info:
        workflow_config = WorkflowConfigSchema(
            context={"workflow_storage": workflow_storage}
        ).load({"workflow_name": "DUMMY_WORKFLOW"})
    assert (
        f"Workflow does not accept parameters {{'{missing_parameter}'}}."
        in exc_info.value.messages["workflow_name"]
    )


def test_workflow_config_schema_no_context():
    """
    Test that WorkflowConfigSchema raises a ValidationError if
    'workflow_storage' is not provided in the schema.
    """
    with pytest.raises(ValidationError) as exc_info:
        workflow_config = WorkflowConfigSchema().load(
            {"workflow_name": "DUMMY_WORKFLOW"}
        )
    assert (
        f"'workflow_storage' was not provided in the context. Cannot check for workflow existence."
        in exc_info.value.messages["workflow_name"]
    )


def test_workflow_config_schema_missing_and_unexpected_parameter_names(tmpdir):
    """
    Test that WorkflowConfigSchema raises a ValidationError if the names of the
    provided parameters are not valid for the named workflow.
    """
    # Create storage object to pass in context
    dummy_workflow = Flow(name="DUMMY_WORKFLOW")
    for parameter in ["reference_date", "date_ranges", "DUMMY_PARAM"]:
        dummy_workflow.add_task(Parameter(parameter))
    workflow_storage = storage.Local(tmpdir)
    workflow_storage.add_flow(dummy_workflow)

    input_dict = dict(
        workflow_name="DUMMY_WORKFLOW", parameters={"EXTRA_PARAM": "DUMMY_VALUE"}
    )
    with pytest.raises(ValidationError) as exc_info:
        workflow_config = WorkflowConfigSchema(
            context={"workflow_storage": workflow_storage}
        ).load(input_dict)
    assert (
        "Missing required parameters {'DUMMY_PARAM'} for workflow 'DUMMY_WORKFLOW'."
        in exc_info.value.messages["parameters"]
    )
    assert (
        "Unexpected parameters provided for workflow 'DUMMY_WORKFLOW': {'EXTRA_PARAM'}."
        in exc_info.value.messages["parameters"]
    )
