# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Defines the NotebookSchema class, for loading a notebook task specification.
"""

from pathlib import Path

from marshmallow import (
    fields,
    Schema,
    validates,
    ValidationError,
)

from autoflow.parser.notebook_output_schema import NotebookOutputSchema


class NotebookSchema(Schema):
    """
    Schema for a notebook task specification.

    Fields
    ------
    filename : str
        Filename of Jupyter notebook to be executed.
    parameters : dict, optional
        Dictionary mapping parameter names (as they will be accessible within
        the notebook) to the names of task outputs within the surrounding workflow.
    output : Nested(NotebookOutputSchema), optional
        Configuration parameters for the output generation task.
    """

    filename = fields.String(required=True)
    parameters = fields.Dict(
        keys=fields.String(), values=fields.String(), required=False
    )
    output = fields.Nested(NotebookOutputSchema, required=False)

    @validates("filename")
    def validate_filename(self, value):
        """
        Raise a ValidationError if the specified file does not exist or is not a Jupyter notebook,
        or if the inputs directory is not specified in the context.
        """
        if Path(value).suffix != ".ipynb":
            raise ValidationError(f"File '{value}' is not a Jupyter notebook.")
        try:
            inputs_dir = Path(self.context["inputs_dir"])
        except KeyError:
            raise ValidationError(
                "'inputs_dir' was not provided in the context. Cannot check for file existence."
            )
        if not (inputs_dir / value).exists():
            raise ValidationError(f"Notebook '{value}' not found.")
