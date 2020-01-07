# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest

from collections import OrderedDict

from marshmallow import Schema, ValidationError

from autoflow.parser.notebooks_field import NotebooksField


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
