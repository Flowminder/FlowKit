# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Defines the WorkflowConfigSchema class, for loading parameters that the sensor will use to run a workflow.
"""

from marshmallow import (
    fields,
    Schema,
    post_load,
    validate,
    validates,
    validates_schema,
    ValidationError,
)

from autoflow.parser.custom_fields import DateField, DateStencilField
from autoflow.sensor import WorkflowConfig


class WorkflowConfigSchema(Schema):
    """
    Schema for parameters that the available dates sensor will use to run a workflow.

    Fields
    ------
    workflow_name : str
        Name of the workflow to run.
    parameters : dict, optional
        Parameters with which the workflow will run.
    earliest_date : date, optional
        Earliest date of CDR data for which the workflow should run.
    date_stencil : list of int, date and/or pairs of int/date, optional
        Date stencil describing a pattern of dates that must be available for the workflow to run.
    """

    # Parameter names that will always be passed to the workflow by the available dates sensor.
    # TODO: Allow automatic parameter names to be specified at schema initialisation, instead of hard-coded.
    _automatic_parameters = ("reference_date", "date_ranges")
    # Fields
    workflow_name = fields.String(required=True)
    parameters = fields.Dict(
        keys=fields.String(validate=validate.NoneOf(_automatic_parameters)),
        required=False,
    )
    earliest_date = DateField(required=False)
    date_stencil = DateStencilField(required=False)

    @validates("workflow_name")
    def validate_workflow(self, value):
        """
        Raise a ValidationError if the named workflow does not exist, or
        doesn't accept parameters 'reference_date' and 'date_ranges'.
        """
        # Check that workflow exists
        try:
            if value not in self.context["workflow_storage"]:
                raise ValidationError("Workflow does not exist in this storage.")
        except KeyError:
            raise ValidationError(
                "'workflow_storage' was not provided in the context. Cannot check for workflow existence."
            )
        # Check that workflow accepts parameters that will automatically be passed when it runs
        workflow_parameter_names = {
            p.name
            for p in self.context["workflow_storage"].get_flow(value).parameters()
        }
        missing_automatic_parameters = set(self._automatic_parameters).difference(
            workflow_parameter_names
        )
        if missing_automatic_parameters:
            raise ValidationError(
                f"Workflow does not accept parameters {missing_automatic_parameters}."
            )

    @validates_schema
    def validate_workflow_parameters(self, data, **kwargs):
        """
        Raise a ValidationError if any required workflow parameters are not
        provided, or if any unexpected parameters are provided.
        """
        errors = {}
        # Parameters workflow expects
        workflow_parameters = (
            self.context["workflow_storage"]
            .get_flow(data["workflow_name"])
            .parameters()
        )
        parameter_names = {p.name for p in workflow_parameters}
        required_parameter_names = {p.name for p in workflow_parameters if p.required}
        # Parameters workflow will receive
        provided_parameter_names = set(data.get("parameters", {}).keys()).union(
            self._automatic_parameters
        )
        # Required parameters that are not provided
        missing_parameters = required_parameter_names.difference(
            provided_parameter_names
        )
        if missing_parameters:
            errors["parameters"] = errors.get("parameters", []) + [
                f"Missing required parameters {missing_parameters} for workflow '{data['workflow_name']}'."
            ]
        # Extra parameters that the workflow is not expecting
        unexpected_parameters = provided_parameter_names.difference(parameter_names)
        if unexpected_parameters:
            errors["parameters"] = errors.get("parameters", []) + [
                f"Unexpected parameters provided for workflow '{data['workflow_name']}': {unexpected_parameters}."
            ]
        if errors:
            raise ValidationError(errors)

    @post_load
    def make_workflow_config(self, data, **kwargs) -> WorkflowConfig:
        """
        Return the provided workflow config parameters in a WorkflowConfig namedtuple.
        """
        return WorkflowConfig(**data)
