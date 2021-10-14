# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Defines the WorkflowSchema class, for loading a notebook-based workflow specification.
"""
from typing import Optional

from marshmallow import (
    fields,
    Schema,
    post_load,
    validates_schema,
    ValidationError,
)
from prefect import storage

from autoflow.parser.notebooks_field import NotebooksField
from autoflow.workflows import make_notebooks_workflow


class WorkflowSchema(Schema):
    """
    Schema for a notebook-based workflow specification.

    Fields
    ------
    name : str
        Name of the prefect flow.
    notebooks : dict
        Dictionary of notebook task specifications.
    """

    name = fields.String(required=True)
    notebooks = NotebooksField(required=True)
    storage_path = fields.String(required=False)

    @validates_schema(pass_many=True)
    def check_for_duplicate_names(self, data, many, **kwargs):
        """
        If this schema is used with 'many=True', raise a ValidationError if any workflow names are duplicated.
        """
        if many:
            errors = {}
            names = set()
            for i, workflow in enumerate(data):
                if workflow["name"] in names:
                    errors[i] = {"name": [f"Duplicate workflow name."]}
                else:
                    names.add(workflow["name"])
            if errors:
                raise ValidationError(errors)
        else:
            pass

    @post_load(pass_many=True)
    def make_and_store_workflows(self, data, many, **kwargs) -> storage.Local:
        """
        Create a prefect flow for each of the provided workflow specifications,
        and return as a prefect 'Local' storage object.
        """

        if not many:
            data = [data]
        storage_path = data[0].get("storage_path")
        workflow_storage = storage.Local(directory=storage_path)
        for workflow_spec in data:
            workflow = make_notebooks_workflow(**workflow_spec)
            workflow_storage.add_flow(workflow)
        return workflow_storage
