# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest

from marshmallow import ValidationError
from prefect import storage

from autoflow.parser.workflow_schema import WorkflowSchema


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
