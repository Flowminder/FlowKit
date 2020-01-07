# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest

from marshmallow import ValidationError

from autoflow.parser.notebook_output_schema import NotebookOutputSchema


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
