# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest

from marshmallow import ValidationError

from autoflow.parser.notebook_schema import NotebookSchema


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
