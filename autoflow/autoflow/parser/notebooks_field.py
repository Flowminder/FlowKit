# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Defines the NotebooksField class, to deserialise a collection of notebook task
specifications as an OrderedDict.
"""

from typing import Any, Dict, OrderedDict

from marshmallow import (
    fields,
    validate,
    ValidationError,
)

from autoflow.parser.notebook_schema import NotebookSchema
from autoflow.utils import sort_notebooks


class NotebooksField(fields.Dict):
    """
    Custom field to validate a collection of notebook task specifications,
    and return them in a topologically-sorted order.
    """

    def __init__(self, **kwargs):
        # Ensure that the 'keys' and 'values' arguments cannot be specified.
        if "keys" in kwargs:
            raise TypeError("The Notebooks field does not accept a 'keys' argument.")
        if "values" in kwargs:
            raise TypeError("The Notebooks field does not accept a 'values' argument.")
        super().__init__(
            keys=fields.String(
                validate=validate.NoneOf(
                    ["reference_date", "date_ranges", "flowapi_url"]
                )
            ),
            values=fields.Nested(NotebookSchema),
            **kwargs,
        )

    def _deserialize(
        self, value, attr, data, **kwargs
    ) -> OrderedDict[str, Dict[str, Any]]:
        """
        Deserialise notebook task specifications as an OrderedDict.

        Returns
        -------
        OrderedDict
            Notebook task specifications, ordered so that no notebook depends
            on another notebook that comes after it.

        Raises
        ------
        ValidationError
            if the notebook task specifications contain circular dependencies.
        """
        notebooks = super()._deserialize(value, attr, data, **kwargs)
        try:
            sorted_notebooks = sort_notebooks(notebooks)
        except ValueError:
            raise ValidationError(
                "Notebook specifications contain circular dependencies."
            )
        return sorted_notebooks
