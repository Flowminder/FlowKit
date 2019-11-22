# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest

import datetime
from collections import OrderedDict
from textwrap import dedent
from unittest.mock import Mock

from marshmallow import fields, Schema, ValidationError
from prefect import Flow, Parameter
from prefect.environments import storage
from prefect.schedules import Schedule

from autoflow.date_stencil import DateStencil
from autoflow.parser import (
    AvailableDatesSensorSchema,
    DateField,
    DateStencilField,
    NotebookOutputSchema,
    NotebookSchema,
    NotebooksField,
    parse_workflows_yaml,
    ScheduleField,
    WorkflowConfigSchema,
    WorkflowSchema,
)
from autoflow.sensor import WorkflowConfig


def test_schedule_field():
    """
    Test that ScheduleField deserialises a cron string to a Schedule object.
    """
    schedule = ScheduleField().deserialize("0 0 * * *")
    assert isinstance(schedule, Schedule)
    assert schedule.clocks[0].cron == "0 0 * * *"


def test_schedule_field_none():
    """
    Test that ScheduleField deserialises None to None.
    """
    schedule = ScheduleField(allow_none=True).deserialize(None)
    assert schedule is None


def test_schedule_field_validation_error():
    """
    Test that ScheduleField raises a ValidationError for an invalid cron string.
    """
    with pytest.raises(ValidationError) as exc_info:
        schedule = ScheduleField().deserialize("NOT A CRON STRING")
    assert "Invalid cron string: 'NOT A CRON STRING'." in exc_info.value.messages


def test_date_field_deserialises_date():
    """
    Test that DateField can deserialise a date object.
    """
    input_date = datetime.date(2016, 1, 1)
    deserialised_date = DateField().deserialize(input_date)
    assert deserialised_date == input_date


def test_date_field_deserialises_string():
    """
    Test that DateField can deserialise an iso date string to a date object.
    """
    deserialised_date = DateField().deserialize("2016-01-01")
    assert deserialised_date == datetime.date(2016, 1, 1)


def test_date_stencil_field():
    """
    Test that DateStencilField deserialises a raw stencil to a DateStencil object.
    """
    raw_stencil = [[datetime.date(2016, 1, 1), datetime.date(2016, 1, 3)], [-2, -1], 0]
    date_stencil = DateStencilField().deserialize(raw_stencil)
    assert isinstance(date_stencil, DateStencil)
    assert date_stencil._intervals == (
        (datetime.date(2016, 1, 1), datetime.date(2016, 1, 3)),
        (-2, -1),
        (0, 0),
    )


@pytest.mark.parametrize(
    "raw_stencil,message",
    [
        ("NOT_A_LIST", "N is not an integer or date."),
        (["BAD_ELEMENT"], "BAD_ELEMENT is not an integer or date."),
        ([[-1, "BAD_ELEMENT"]], "BAD_ELEMENT is not an integer or date."),
        (
            [[-3, -2, -1]],
            "Expected date interval to have length 2 (in format [start, end]), but got sequence of length 3.",
        ),
        ([[-1, -2]], "Date stencil contains invalid interval (-1, -2)."),
    ],
)
def test_date_stencil_field_validation_error(raw_stencil, message):
    """
    Test that DateStencilField raises a ValidationError if the raw stencil is not valid.
    """
    with pytest.raises(ValidationError) as exc_info:
        date_stencil = DateStencilField().deserialize(raw_stencil)
    assert message in exc_info.value.messages


def test_notebook_output_schema(monkeypatch):
    """
    Test that NotebookOutputSchema can load a dict of notebook output options.
    """
    monkeypatch.setattr("pathlib.Path.exists", lambda self: True)
    input_value = {"format": "pdf", "template": "DUMMY_TEMPLATE"}
    loaded_value = NotebookOutputSchema(
        context={"inputs_dir": "DUMMY_INPUTS_DIR"}
    ).load(input_value)
    assert loaded_value == input_value


def test_notebook_output_schema_no_template():
    """
    Test that NotebookOutputSchema can load notebook output options if
    'template' is not specified.
    """
    loaded_value = NotebookOutputSchema().load({"format": "pdf"})
    assert loaded_value == {"format": "pdf", "template": None}


def test_notebook_output_schema_no_format():
    """
    Test that NotebookOutputSchema raises a ValidationError if 'format' is not specified.
    """
    with pytest.raises(ValidationError) as exc_info:
        loaded_value = NotebookOutputSchema().load({})
    assert "Missing data for required field." in exc_info.value.messages["format"]


def test_notebook_output_schema_invalid_format():
    """
    Test that NotebookOutputSchema raises a ValidationError if 'format' is not 'pdf'.
    """
    with pytest.raises(ValidationError) as exc_info:
        loaded_value = NotebookOutputSchema().load({"format": "not_pdf"})
    assert "Must be one of: pdf." in exc_info.value.messages["format"]


def test_notebook_output_schema_file_not_found():
    """
    Test that NotebookOutputSchema raises a ValidationError if the template
    file doesn't exist.
    """
    with pytest.raises(ValidationError) as exc_info:
        loaded_value = NotebookOutputSchema(
            context={"inputs_dir": "DUMMY_INPUTS_DIR"}
        ).load({"format": "pdf", "template": "DUMMY_TEMPLATE"})
    assert (
        "Asciidoc template file 'DUMMY_TEMPLATE' not found."
        in exc_info.value.messages["template"]
    )


def test_notebook_output_schema_no_context():
    """
    Test that NotebookOutputSchema raises a ValidationError if 'inputs_dir'
    isn't provided in the context.
    """
    with pytest.raises(ValidationError) as exc_info:
        loaded_value = NotebookOutputSchema().load(
            {"format": "pdf", "template": "DUMMY_TEMPLATE"}
        )
    assert (
        "'inputs_dir' was not provided in the context. Cannot check for file existence."
        in exc_info.value.messages["template"]
    )


def test_notebook_schema(monkeypatch):
    """
    Test that NotebookSchema can load a notebook task specification.
    """
    monkeypatch.setattr("pathlib.Path.exists", lambda self: True)
    input_value = {
        "filename": "DUMMY_NOTEBOOK.ipynb",
        "parameters": {"DUMMY_PARAM": "DUMMY_VALUE"},
        "output": {"format": "pdf", "template": "DUMMY_TEMPLATE"},
    }
    loaded_value = NotebookSchema(context={"inputs_dir": "DUMMY_INPUTS_DIR"}).load(
        input_value
    )
    assert loaded_value == input_value


def test_notebook_schema_optional_fields(monkeypatch):
    """
    Test that NotebookSchema can load a notebook task specification without
    'parameters' or 'output' specified.
    """
    monkeypatch.setattr("pathlib.Path.exists", lambda self: True)
    input_value = {"filename": "DUMMY_NOTEBOOK.ipynb"}
    loaded_value = NotebookSchema(context={"inputs_dir": "DUMMY_INPUTS_DIR"}).load(
        input_value
    )
    assert loaded_value == input_value


def test_notebook_schema_no_filename(monkeypatch):
    """
    Test that NotebookSchema raises a ValidationError if filename is not provided.
    """
    input_value = {"parameters": {"DUMMY_PARAM": "DUMMY_VALUE"}}
    with pytest.raises(ValidationError) as exc_info:
        loaded_value = NotebookSchema().load(input_value)
    assert "Missing data for required field." in exc_info.value.messages["filename"]


def test_notebook_schema_invalid_filename(monkeypatch):
    """
    Test that NotebookSchema raises a ValidationError if the file is not a Jupyter notebook.
    """
    monkeypatch.setattr("pathlib.Path.exists", lambda self: True)
    input_value = {"filename": "NOT_A_NOTEBOOK.html"}
    with pytest.raises(ValidationError) as exc_info:
        loaded_value = NotebookSchema(context={"inputs_dir": "DUMMY_INPUTS_DIR"}).load(
            input_value
        )
    assert (
        "File 'NOT_A_NOTEBOOK.html' is not a Jupyter notebook."
        in exc_info.value.messages["filename"]
    )


def test_notebook_schema_file_not_found():
    """
    Test that NotebookSchema raises a ValidationError if the file does not exist.
    """
    input_value = {"filename": "DUMMY_NOTEBOOK.ipynb"}
    with pytest.raises(ValidationError) as exc_info:
        loaded_value = NotebookSchema(context={"inputs_dir": "DUMMY_INPUTS_DIR"}).load(
            input_value
        )
    assert (
        "Notebook 'DUMMY_NOTEBOOK.ipynb' not found."
        in exc_info.value.messages["filename"]
    )


def test_notebook_schema_no_context():
    """
    Test that NotebookSchema raises a ValidationError if 'inputs_dir'
    isn't provided in the context.
    """
    input_value = {"filename": "DUMMY_NOTEBOOK.ipynb"}
    with pytest.raises(ValidationError) as exc_info:
        loaded_value = NotebookSchema().load(input_value)
    assert (
        "'inputs_dir' was not provided in the context. Cannot check for file existence."
        in exc_info.value.messages["filename"]
    )


def test_notebook_schema_parameters_keys():
    """
    Test that NotebookSchema raises a ValidationError if parameter keys are not strings.
    """
    input_value = {"filename": "DUMMY_NOTEBOOK.ipynb", "parameters": {1: "DUMMY_VALUE"}}
    with pytest.raises(ValidationError) as exc_info:
        loaded_value = NotebookSchema().load(input_value)
    assert "Not a valid string." in exc_info.value.messages["parameters"][1]["key"]


def test_notebook_schema_parameters_values():
    """
    Test that NotebookSchema raises a ValidationError if parameter values are not strings.
    """
    input_value = {"filename": "DUMMY_NOTEBOOK.ipynb", "parameters": {"DUMMY_PARAM": 1}}
    with pytest.raises(ValidationError) as exc_info:
        loaded_value = NotebookSchema().load(input_value)
    assert (
        "Not a valid string."
        in exc_info.value.messages["parameters"]["DUMMY_PARAM"]["value"]
    )


def test_notebooks_field_init_does_not_accept_keys():
    """
    Test that NotebooksField.__init__ does not accept a 'keys' argument.
    """
    with pytest.raises(
        TypeError, match="The Notebooks field does not accept a 'keys' argument."
    ):
        field = NotebooksField(keys=str)


def test_notebooks_field_init_does_not_accept_values():
    """
    Test that NotebooksField.__init__ does not accept a 'values' argument.
    """
    with pytest.raises(
        TypeError, match="The Notebooks field does not accept a 'values' argument."
    ):
        field = NotebooksField(values=str)


def test_notebooks_field_deserialise(monkeypatch):
    """
    Test that NotebooksField deserialises a dict of notebook specifications as
    an OrderedDict.
    """
    monkeypatch.setattr("pathlib.Path.exists", lambda self: True)
    notebooks = {
        "notebook1": {"filename": "NOTEBOOK1.ipynb"},
        "notebook2": {
            "filename": "NOTEBOOK2.ipynb",
            "parameters": {"other_notebook": "notebook1"},
        },
    }
    notebooks_field = NotebooksField()
    # Can't set context directly on a Field - must be set on the parent Schema
    notebooks_field._bind_to_schema(
        "notebooks", Schema(context={"inputs_dir": "DUMMY_INPUTS_DIR"})
    )
    deserialised_notebooks = notebooks_field.deserialize(notebooks)
    assert isinstance(deserialised_notebooks, OrderedDict)
    assert deserialised_notebooks == notebooks


@pytest.mark.parametrize(
    "key,message",
    [
        (123, "Not a valid string."),
        ("reference_date", "Invalid input."),
        ("date_ranges", "Invalid input."),
        ("flowapi_url", "Invalid input."),
    ],
)
def test_notebooks_field_invalid_keys(monkeypatch, key, message):
    """
    Test that NotebooksField raises a ValidationError if a notebook key is not
    a string, or has a disallowed value.
    """
    monkeypatch.setattr("pathlib.Path.exists", lambda self: True)
    notebooks = {key: {"filename": "NOTEBOOK1.ipynb"}}
    notebooks_field = NotebooksField()
    # Can't set context directly on a Field - must be set on the parent Schema
    notebooks_field._bind_to_schema(
        "notebooks", Schema(context={"inputs_dir": "DUMMY_INPUTS_DIR"})
    )
    with pytest.raises(ValidationError) as exc_info:
        deserialised_notebooks = notebooks_field.deserialize(notebooks)
    assert message in exc_info.value.messages[key]["key"]


def test_notebooks_field_circular_dependency(monkeypatch):
    """
    Test that NotebooksField raises a ValidationError if notebook
    specifications have circular dependencies.
    """
    monkeypatch.setattr("pathlib.Path.exists", lambda self: True)
    notebooks = {
        "notebook1": {
            "filename": "NOTEBOOK1.ipynb",
            "parameters": {"param": "notebook2"},
        },
        "notebook2": {
            "filename": "NOTEBOOK2.ipynb",
            "parameters": {"param": "notebook1"},
        },
    }
    notebooks_field = NotebooksField()
    # Can't set context directly on a Field - must be set on the parent Schema
    notebooks_field._bind_to_schema(
        "notebooks", Schema(context={"inputs_dir": "DUMMY_INPUTS_DIR"})
    )
    with pytest.raises(ValidationError) as exc_info:
        deserialised_notebooks = notebooks_field.deserialize(notebooks)
    assert (
        "Notebook specifications contain circular dependencies."
        in exc_info.value.messages
    )


def test_workflow_schema(monkeypatch):
    """
    Test that WorkflowSchema loads a workflow specification as a Storage object
    containing the defined flow.
    """
    monkeypatch.setattr("pathlib.Path.exists", lambda self: True)
    workflow = {
        "name": "DUMMY_WORKFLOW",
        "notebooks": {"notebook1": {"filename": "NOTEBOOK1.ipynb"}},
    }
    workflow_storage = WorkflowSchema(context={"inputs_dir": "DUMMY_INPUTS_DIR"}).load(
        workflow
    )
    assert isinstance(workflow_storage, storage.Storage)
    assert "DUMMY_WORKFLOW" in workflow_storage


def test_workflow_schema_missing_name(monkeypatch):
    """
    Test that WorkflowSchema raises a ValidationError if the 'name' field is missing.
    """
    monkeypatch.setattr("pathlib.Path.exists", lambda self: True)
    workflow = {"notebooks": {"notebook1": {"filename": "NOTEBOOK1.ipynb"}}}
    with pytest.raises(ValidationError) as exc_info:
        workflow_storage = WorkflowSchema(
            context={"inputs_dir": "DUMMY_INPUTS_DIR"}
        ).load(workflow)
    assert "Missing data for required field." in exc_info.value.messages["name"]


def test_workflow_schema_invalid_name(monkeypatch):
    """
    Test that WorkflowSchema raises a ValidationError if the 'name' field is not a string.
    """
    monkeypatch.setattr("pathlib.Path.exists", lambda self: True)
    workflow = {
        "name": 123,
        "notebooks": {"notebook1": {"filename": "NOTEBOOK1.ipynb"}},
    }
    with pytest.raises(ValidationError) as exc_info:
        workflow_storage = WorkflowSchema(
            context={"inputs_dir": "DUMMY_INPUTS_DIR"}
        ).load(workflow)
    assert "Not a valid string." in exc_info.value.messages["name"]


def test_workflow_schema_missing_notebooks(monkeypatch):
    """
    Test that WorkflowSchema raises a ValidationError if the 'notebooks' field is missing.
    """
    monkeypatch.setattr("pathlib.Path.exists", lambda self: True)
    workflow = {"name": "DUMMY_WORKFLOW"}
    with pytest.raises(ValidationError) as exc_info:
        workflow_storage = WorkflowSchema(
            context={"inputs_dir": "DUMMY_INPUTS_DIR"}
        ).load(workflow)
    assert "Missing data for required field." in exc_info.value.messages["notebooks"]


def test_workflow_schema_many(monkeypatch):
    """
    Test that WorkflowSchema can load multiple workflow specifications as a
    single Storage object.
    """
    monkeypatch.setattr("pathlib.Path.exists", lambda self: True)
    workflows = [
        {
            "name": "DUMMY_WORKFLOW_1",
            "notebooks": {"notebook1": {"filename": "NOTEBOOK1.ipynb"}},
        },
        {
            "name": "DUMMY_WORKFLOW_2",
            "notebooks": {"notebook2": {"filename": "NOTEBOOK2.ipynb"}},
        },
    ]
    workflow_storage = WorkflowSchema(
        many=True, context={"inputs_dir": "DUMMY_INPUTS_DIR"}
    ).load(workflows)
    assert isinstance(workflow_storage, storage.Storage)
    assert "DUMMY_WORKFLOW_1" in workflow_storage
    assert "DUMMY_WORKFLOW_2" in workflow_storage


def test_workflow_schema_duplicate_name(monkeypatch):
    """
    Test that WorkflowSchema raises a ValidationError if multiple workflows
    have the same name.
    """
    monkeypatch.setattr("pathlib.Path.exists", lambda self: True)
    workflows = [
        {
            "name": "DUMMY_WORKFLOW",
            "notebooks": {"notebook1": {"filename": "NOTEBOOK1.ipynb"}},
        },
        {
            "name": "DUMMY_WORKFLOW",
            "notebooks": {"notebook2": {"filename": "NOTEBOOK2.ipynb"}},
        },
        {
            "name": "DUMMY_WORKFLOW",
            "notebooks": {"notebook3": {"filename": "NOTEBOOK3.ipynb"}},
        },
    ]
    with pytest.raises(ValidationError) as exc_info:
        workflow_storage = WorkflowSchema(
            many=True, context={"inputs_dir": "DUMMY_INPUTS_DIR"}
        ).load(workflows)
    assert "Duplicate workflow name." in exc_info.value.messages[1]["name"]
    assert "Duplicate workflow name." in exc_info.value.messages[2]["name"]


def test_workflow_config_schema():
    """
    Test that WorkflowConfigSchema loads input data into a WorkflowConfig namedtuple.
    """
    # Create storage object to pass in context
    dummy_workflow = Flow(name="DUMMY_WORKFLOW")
    for parameter in ["reference_date", "date_ranges", "DUMMY_PARAM"]:
        dummy_workflow.add_task(Parameter(parameter))
    workflow_storage = storage.Memory()
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


def test_workflow_config_schema_defaults():
    """
    Test that WorkflowConfigSchema loads input data if 'parameters',
    'earliest_date' and 'date_stencil' are not specified.
    """
    # Create storage object to pass in context
    dummy_workflow = Flow(name="DUMMY_WORKFLOW")
    for parameter in ["reference_date", "date_ranges"]:
        dummy_workflow.add_task(Parameter(parameter))
    workflow_storage = storage.Memory()
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
def test_workflow_config_schema_invalid_parameter_names(key):
    """
    Test that WorkflowConfigSchema raises a ValidationError if the 'parameters'
    dict keys contain 'reference_date' or 'date_ranges'.
    """
    # Create storage object to pass in context
    dummy_workflow = Flow(name="DUMMY_WORKFLOW")
    for parameter in ["reference_date", "date_ranges"]:
        dummy_workflow.add_task(Parameter(parameter))
    workflow_storage = storage.Memory()
    workflow_storage.add_flow(dummy_workflow)

    input_dict = dict(workflow_name="DUMMY_WORKFLOW", parameters={key: "DUMMY_VALUE"})
    with pytest.raises(ValidationError) as exc_info:
        workflow_config = WorkflowConfigSchema(
            context={"workflow_storage": workflow_storage}
        ).load(input_dict)
    assert "Invalid input." in exc_info.value.messages["parameters"][key]["key"]


def test_workflow_config_schema_invalid_earliest_date():
    """
    Test that WorkflowConfigSchema raises a ValidationError if the
    'earliest_date' field is not a date.
    """
    # Create storage object to pass in context
    dummy_workflow = Flow(name="DUMMY_WORKFLOW")
    for parameter in ["reference_date", "date_ranges"]:
        dummy_workflow.add_task(Parameter(parameter))
    workflow_storage = storage.Memory()
    workflow_storage.add_flow(dummy_workflow)

    input_dict = dict(workflow_name="DUMMY_WORKFLOW", earliest_date=datetime.time(11))
    with pytest.raises(ValidationError) as exc_info:
        workflow_config = WorkflowConfigSchema(
            context={"workflow_storage": workflow_storage}
        ).load(input_dict)
    assert "Not a valid date." in exc_info.value.messages["earliest_date"]


def test_workflow_config_schema_workflow_not_found():
    """
    Test that WorkflowConfigSchema raises a ValidationError if the named
    workflow does not exist.
    """
    # Create storage object to pass in context
    dummy_workflow = Flow(name="DUMMY_WORKFLOW")
    for parameter in ["reference_date", "date_ranges"]:
        dummy_workflow.add_task(Parameter(parameter))
    workflow_storage = storage.Memory()
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
    missing_parameter,
):
    """
    Test that WorkflowConfigSchema raises a ValidationError if the named
    workflow does not accept parameters 'reference_date' and 'date_ranges'.
    """
    # Create storage object to pass in context
    dummy_workflow = Flow(name="DUMMY_WORKFLOW")
    for parameter in {"reference_date", "date_ranges"} - {missing_parameter}:
        dummy_workflow.add_task(Parameter(parameter))
    workflow_storage = storage.Memory()
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


def test_workflow_config_schema_missing_and_unexpected_parameter_names():
    """
    Test that WorkflowConfigSchema raises a ValidationError if the names of the
    provided parameters are not valid for the named workflow.
    """
    # Create storage object to pass in context
    dummy_workflow = Flow(name="DUMMY_WORKFLOW")
    for parameter in ["reference_date", "date_ranges", "DUMMY_PARAM"]:
        dummy_workflow.add_task(Parameter(parameter))
    workflow_storage = storage.Memory()
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


def test_available_dates_sensor_schema():
    """
    Test that AvailableDatesSensorSchema can load a valid set of sensor config parameters.
    """
    # Create storage object to pass in context
    dummy_workflow = Flow(name="DUMMY_WORKFLOW")
    for parameter in ["reference_date", "date_ranges", "DUMMY_PARAM"]:
        dummy_workflow.add_task(Parameter(parameter))
    workflow_storage = storage.Memory()
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


def test_parse_workflows_yaml(tmp_path):
    """
    Test that parse_workflows_yaml correctly parses an example input file.
    """
    (tmp_path / "notebook1.ipynb").touch()
    (tmp_path / "notebook2.ipynb").touch()
    (tmp_path / "notebook3.ipynb").touch()
    (tmp_path / "custom_template.tpl").touch()
    (tmp_path / "dummy_input.yml").write_text(
        dedent(
            """\
            workflows:
              - name: workflow1
                notebooks:
                  notebook1:
                    filename: notebook1.ipynb
                    parameters:
                      url: flowapi_url
                      date: reference_date
                      extra: dummy_param
                  notebook2:
                    filename: notebook2.ipynb
                    parameters:
                      ranges: date_ranges
                      other: notebook1
                    output:
                      format: pdf
                      template: custom_template.tpl
              - name: workflow2
                notebooks:
                  the_notebook:
                    filename: notebook3.ipynb
                    output:
                      format: pdf
            
            available_dates_sensor:
              schedule: "0 0 * * *"
              cdr_types:
                - calls
                - sms
              workflows:
                - workflow_name: workflow1
                  parameters:
                    dummy_param: 123
                  earliest_date: 2016-01-01
                  date_stencil: [[2016-01-01, 2016-01-02], -1, 0]
                - workflow_name: workflow2
            """
        )
    )
    workflow_storage, sensor_config = parse_workflows_yaml(
        filename="dummy_input.yml", inputs_dir=str(tmp_path)
    )
    assert isinstance(workflow_storage, storage.Storage)
    assert "workflow1" in workflow_storage
    assert "workflow2" in workflow_storage
    assert isinstance(sensor_config["schedule"], Schedule)
    assert sensor_config["cdr_types"] == ["calls", "sms"]
    assert sensor_config["workflows"] == [
        WorkflowConfig(
            workflow_name="workflow1",
            parameters={"dummy_param": 123},
            earliest_date=datetime.date(2016, 1, 1),
            date_stencil=DateStencil(
                [[datetime.date(2016, 1, 1), datetime.date(2016, 1, 2)], -1, 0]
            ),
        ),
        WorkflowConfig(workflow_name="workflow2"),
    ]


def test_parse_workflows_yaml_missing_workflows(tmp_path):
    """
    Test that parse_workflows_yaml raises a ValueError if the input file
    doesn't have a 'workflows' key.
    """
    (tmp_path / "dummy_input.yml").write_text(
        dedent(
            """\
            available_dates_sensor:
              schedule: "0 0 * * *"
              workflows:
                - workflow_name: workflow1
            """
        )
    )
    with pytest.raises(
        ValueError, match="Input file does not have a 'workflows' section."
    ):
        workflow_storage, sensor_config = parse_workflows_yaml(
            filename="dummy_input.yml", inputs_dir=str(tmp_path)
        )


def test_parse_workflows_yaml_missing_available_dates_sensor(tmp_path):
    """
    Test that parse_workflows_yaml raises a ValueError if the input file
    doesn't have a 'workflows' key.
    """
    (tmp_path / "notebook1.ipynb").touch()
    (tmp_path / "dummy_input.yml").write_text(
        dedent(
            """\
            workflows:
              - name: workflow1
                notebooks:
                  notebook1:
                    filename: notebook1.ipynb
            """
        )
    )
    with pytest.raises(
        ValueError,
        match="Input file does not have an 'available_dates_sensor' section.",
    ):
        workflow_storage, sensor_config = parse_workflows_yaml(
            filename="dummy_input.yml", inputs_dir=str(tmp_path)
        )
