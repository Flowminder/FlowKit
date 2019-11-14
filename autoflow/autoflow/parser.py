# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import datetime
import yaml
from marshmallow import (
    fields,
    Schema,
    post_load,
    validate,
    validates,
    validates_schema,
    ValidationError,
)
from pathlib import Path
from prefect.environments import storage
from prefect.schedules import CronSchedule

from .date_stencil import DateStencil, InvalidDateIntervalError
from .sensor import WorkflowConfig
from .utils import sort_notebooks
from .workflows import make_notebooks_workflow


# Fields ----------------------------------------------------------------------


class NotebooksField(fields.Dict):
    def __init__(self, keys=None, values=None, **kwargs):
        if "keys" in kwargs:
            raise TypeError("The Notebooks field does not accept a 'keys' argument.")
        if "values" in kwargs:
            raise ValueError("The Notebooks field does not accept a 'values' argument.")
        super().__init__(
            keys=fields.String(
                validate=validate.NoneOf(
                    ["reference_date", "date_ranges", "flowapi_url"]
                )
            ),
            values=fields.Nested(NotebookSchema),
            **kwargs,
        )

    def _deserialize(self, value, attr, data, **kwargs):
        notebooks = super()._deserialize(value, attr, data, **kwargs)
        try:
            sorted_notebooks = sort_notebooks(notebooks)
        except ValueError:
            raise ValidationError(
                "Notebook specifications contain circular dependencies."
            )
        return sorted_notebooks


class ScheduleField(fields.String):
    def _deserialize(self, value, attr, data, **kwargs):
        if value is None:
            return value
        else:
            cron_string = super()._deserialize(value, attr, data, **kwargs)
            try:
                schedule = CronSchedule(cron_string)
            except ValueError:
                raise ValidationError(f"Invalid cron string: '{cron_string}'.")
            return schedule


class DateField(fields.Date):
    # yaml.safe_load converts iso-format date strings to date objects,
    # so we need a field that can 'deserialise' dates that are already dates.
    def _deserialize(self, value, attr, data, **kwargs):
        if isinstance(value, datetime.date):
            return value
        else:
            return super()._deserialize(value, attr, data, **kwargs)


class DateStencilField(fields.Field):
    def _deserialize(self, value, attr, data, **kwargs):
        try:
            return DateStencil(raw_stencil=value)
        except (TypeError, ValueError, InvalidDateIntervalError) as e:
            raise ValidationError(str(e))


# Schemas ----------------------------------------------------------------------


class NotebookOutputSchema(Schema):
    format = fields.String(validate=OneOf["pdf"], required=True)
    template = fields.String(missing=None)

    @validates("template")
    def validate_template(self, value):
        try:
            inputs_dir = Path(self.context["inputs_dir"])
        except KeyError:
            raise ValidationError(
                "'inputs_dir' was not provided in the context. Cannot check for file existence."
            )
        if not (inputs_dir / value).exists():
            raise ValidationError(f"Asciidoc template file '{value}' not found.")


class NotebookSchema(Schema):
    filename = fields.String(required=True)
    parameters = fields.Dict(keys=fields.String(), values=fields.String(), missing=None)
    output = fields.Nested(NotebookOutputSchema, required=False)

    @validates("filename")
    def validate_filename(self, value):
        if Path(value).suffix != ".ipynb":
            # TODO: '.ipynb' does not a notebook make
            raise ValidationError(f"File '{value}' is not a Jupyter notebook.")
        try:
            inputs_dir = Path(self.context["inputs_dir"])
        except KeyError:
            raise ValidationError(
                "'inputs_dir' was not provided in the context. Cannot check for file existence."
            )
        if not (inputs_dir / value).exists():
            raise ValidationError(f"Notebook '{value}' not found.")


class WorkflowSchema(Schema):
    name = fields.String(required=True)
    notebooks = NotebooksField()

    @validates_schema(pass_many=True)
    def check_for_duplicate_names(self, data, many, **kwargs):
        if many:
            names = set()
            for workflow in data:
                if workflow["name"] in names:
                    raise ValidationError(
                        f"Duplicate workflow name: {workflow['name']}."
                    )
                else:
                    names.add(workflow["name"])
        else:
            pass

    @post_load(pass_many=True)
    def make_and_store_workflows(self, data, many, **kwargs):
        workflow_storage = storage.Memory()
        if not many:
            data = [data]
        for workflow_spec in data:
            workflow = make_notebooks_workflow(**workflow_spec)
            workflow_storage.add_flow(workflow)
        return workflow_storage


class WorkflowConfigSchema(Schema):
    workflow_name = fields.String(required=True)
    parameters = fields.Dict(keys=fields.String(), missing=None)
    earliest_date = DateField(required=False)
    date_stencil = DateStencilField(required=False)

    @validates("workflow_name")
    def validate_workflow(self, value):
        # Check that workflow exists
        try:
            if value not in self.context["workflow_storage"]:
                raise ValidationError(
                    f"Workflow '{value}' does not exist in this storage."
                )
        except KeyError:
            raise ValidationError(
                "'workflow_storage' was not provided in the context. Cannot check for workflow existence."
            )
        # Check that workflow accepts parameters that will automatically be passed when it runs
        workflow_parameter_names = {
            p.name
            for p in self.context["workflow_storage"].get_flow(value).parameters()
        }
        automatic_parameters = set(self.context.get("automatic_parameters", ()))
        missing_automatic_parameters = automatic_parameters.difference(
            workflow_parameter_names
        )
        if missing_automatic_parameters:
            raise ValidationError(
                f"Workflow '{value}' does not accept parameters {missing_automatic_parameters}."
            )

    @validates("parameters")
    def check_automatic_parameters_not_provided(self, value):
        for param in self.context.get("automatic_parameters", ()):
            if value is not None and param in value.keys():
                raise ValidationError(
                    f"Parameter '{param}' is provided to the workflow automatically, so cannot be set manually."
                )

    @validates_schema
    def validate_workflow_parameters(self, data, **kwargs):
        # Parameters workflow expects
        workflow_parameters = (
            self.context["workflow_storage"]
            .get_flow(data["workflow_name"])
            .parameters()
        )
        parameter_names = {p.name for p in workflow_parameters}
        required_parameter_names = {p.name for p in workflow_parameters if p.required}
        # Parameters workflow will receive
        provided_parameter_names = (
            set() if data["parameters"] is None else set(data["parameters"].keys())
        ).union(self.context.get("automatic_parameters", ()))
        # Required parameters that are not provided
        missing_parameters = required_parameter_names.difference(
            provided_parameter_names
        )
        if missing_parameters:
            raise ValidationError(
                {
                    "parameters": f"Missing required parameters {missing_parameters} for workflow '{data['workflow_name']}'."
                }
            )
        # Extra parameters that the workflow is not expecting
        unexpected_parameters = provided_parameter_names.difference(parameter_names)
        if unexpected_parameters:
            raise ValidationError(
                {
                    "parameters": f"Unexpected parameters provided for workflow '{data['workflow_name']}': {unexpected_parameters}."
                }
            )

    @post_load
    def make_workflow_config(self, data, **kwargs):
        return WorkflowConfig(**data)


class AvailableDatesSensorSchema(Schema):
    schedule = ScheduleField(required=True, allow_none=True)
    cdr_types = fields.List(
        fields.String(validate=OneOf(["calls", "sms", "mds", "topups"])), missing=None
    )
    workflows = fields.List(fields.Nested(WorkflowConfigSchema), required=True)


def parse_workflows_yaml(
    filename: str, inputs_dir: str
) -> Tuple[List["prefect.Flow"], Dict[str, List[Dict[str, Any]]]]:
    """
    Construct workflows defined in an input file.

    Parameters
    ----------
    filename : str
        Name of yaml input file
    inputs_dir : Path
        Directory in which input files should be found
    
    Returns
    -------
    workflows : list of Flow
        List of prefect workflows
    run_parameters : dict
        mapping from workflow names to a list of dicts of parameters for which the workflow should be run
    """
    with open(Path(inputs_dir) / filename, "r") as f:
        workflows_yaml = yaml.safe_load(f)

    try:
        workflows_spec = workflows_yaml["workflows"]
    except KeyError:
        raise ValueError("Input file does not have a 'workflows' section.")
    try:
        sensor_spec = workflows_yaml["available_dates_sensor"]
    except KeyError:
        raise ValueError(
            "Input file does not have an 'available_dates_sensor' section."
        )

    workflow_schema = WorkflowSchema(context=dict(inputs_dir=inputs_dir))
    workflow_storage = workflow_schema.load(workflows_spec, many=True)

    sensor_schema = AvailableDatesSensorSchema(
        context=dict(
            workflow_storage=workflow_storage,
            automatic_parameters=["reference_date", "date_ranges"],
        )
    )
    sensor_config = sensor_schema.load(sensor_spec)

    return workflow_storage, sensor_config
