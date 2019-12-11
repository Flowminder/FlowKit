# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Defines the NotebookOutputSchema class, for loading the 'output' parameter of a
notebook task specification.
"""

from pathlib import Path

from marshmallow import (
    fields,
    Schema,
    validate,
    validates,
    ValidationError,
)


class NotebookOutputSchema(Schema):
    """
    Schema for the 'output' parameter of a notebook task specification.

    Fields
    ------
    format : str
        Format to which notebook should be converted (only 'pdf' is currently supported).
    template : str, optional
        Filename of a template file to be used when converting the notebook to ASCIIDoc format.
        If not provided, deserialises to None.
    """

    format = fields.String(validate=validate.OneOf(["pdf"]), required=True)
    template = fields.String(missing=None)

    @validates("template")
    def validate_template(self, value):
        """
        Raise a ValidationError if the specified template file does not exist,
        or if the inputs directory is not specified in the context.
        """
        if value is None:
            return
        try:
            inputs_dir = Path(self.context["inputs_dir"])
        except KeyError:
            raise ValidationError(
                "'inputs_dir' was not provided in the context. Cannot check for file existence."
            )
        if not (inputs_dir / value).exists():
            raise ValidationError(f"Asciidoc template file '{value}' not found.")
