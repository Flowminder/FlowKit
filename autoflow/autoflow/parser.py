# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Defines the function 'parse_workflows_yaml', and marshmallow schemas, for parsing
a 'workflows.yml' file to define workflows and configure the available dates sensor.
"""

import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, OrderedDict, Tuple, Union

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
from prefect.environments import storage
from prefect.schedules import CronSchedule

from autoflow.date_stencil import DateStencil, InvalidDateIntervalError
from autoflow.sensor import WorkflowConfig
from autoflow.utils import sort_notebooks
from autoflow.workflows import make_notebooks_workflow


class ScheduleField(fields.String):
    """
    Custom field to deserialise a cron string as a prefect CronSchedule.
    """

    def _deserialize(self, value, attr, data, **kwargs) -> Optional[CronSchedule]:
        """
        Deserialise a cron string as a cron schedule.

        Returns
        -------
        CronSchedule or None
            If input value is None, returns None (i.e. no schedule), otherwise
            returns a prefect Schedule to run a flow according to the schedule
            defined by the input string.
        
        Raises
        ------
        ValidationError
            if the input value is not a valid cron string or None
        """
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
    """
    Custom field to deserialise a date, which will leave the input value unchanged
    if it is already a date object.
    """

    # yaml.safe_load converts iso-format date strings to date objects,
    # so we need a field that can 'deserialise' dates that are already dates.
    def _deserialize(self, value, attr, data, **kwargs) -> datetime.date:
        """
        If the input value is a date object, return it unchanged.
        Otherwise use marshmallow.fields.Date to deserialise.
        """
        if isinstance(value, datetime.date):
            return value
        else:
            return super()._deserialize(value, attr, data, **kwargs)


class DateStencilField(fields.Field):
    """
    Custom field to deserialise a 'raw' date stencil (i.e. a list of dates, offsets and/or pairs)
    to a DateStencil object.
    """

    def _deserialize(self, value, attr, data, **kwargs) -> DateStencil:
        """
        Create a DateStencil object from the input value.

        Returns
        -------
        DateStencil

        Raises
        ------
        ValidationError
            if the input value is not a valid 'raw' stencil

        See also
        --------
        autoflow.date_stencil.DateStencil
        """
        try:
            return DateStencil(raw_stencil=value)
        except (TypeError, ValueError, InvalidDateIntervalError) as e:
            raise ValidationError(str(e))


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


class NotebooksField(fields.Dict):
    """
    Custom field to validate a collection of notebook task specifications,
    and return them in a topologically-sorted order.
    """

    def __init__(self, keys=None, values=None, **kwargs):
        # Ensure that the 'keys' and 'values' arguments cannot be specified.
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

    @validates_schema(pass_many=True)
    def check_for_duplicate_names(self, data, many, **kwargs):
        """
        If this schema is used with 'many=True', raise a VlidationError if any workflow names are duplicated.
        """
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
    def make_and_store_workflows(self, data, many, **kwargs) -> storage.Memory:
        """
        Create a prefect flow for each of the provided workflow specifications,
        and return as a prefect 'Memory' storage object.
        """
        workflow_storage = storage.Memory()
        if not many:
            data = [data]
        for workflow_spec in data:
            workflow = make_notebooks_workflow(**workflow_spec)
            workflow_storage.add_flow(workflow)
        return workflow_storage


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
        missing_automatic_parameters = set(self._automatic_parameters).difference(
            workflow_parameter_names
        )
        if missing_automatic_parameters:
            raise ValidationError(
                f"Workflow '{value}' does not accept parameters {missing_automatic_parameters}."
            )

    @validates_schema
    def validate_workflow_parameters(self, data, **kwargs):
        """
        Raise a ValidationError if any required workflow parameters are not
        provided, or if any unexpected parameters are provided.
        """
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
    def make_workflow_config(self, data, **kwargs) -> WorkflowConfig:
        """
        Return the provided workflow config parameters in a WorkflowConfig named tuple.
        """
        return WorkflowConfig(**data)


class AvailableDatesSensorSchema(Schema):
    """
    Schema for configuration parameters for the available dates sensor.

    Fields
    ------
    schedule : str or None
        Cron string describing the schedule on which the sensor should check
        available dates, or None for no schedule.
    cdr_types : list of str, optional
        Subset of CDR types for which available dates should be found.
        If not provided, defaults to None.
    workflows : list of Nested(WorkflowConfigSchema)
        List of workflows (and associated parameters) that the sensor should run.
    """

    schedule = ScheduleField(required=True, allow_none=True)
    cdr_types = fields.List(
        fields.String(validate=validate.OneOf(["calls", "sms", "mds", "topups"])),
        missing=None,
    )
    workflows = fields.List(fields.Nested(WorkflowConfigSchema), required=True)


def parse_workflows_yaml(
    filename: str, inputs_dir: str
) -> Tuple[
    "prefect.environments.storage.Storage",
    Dict[str, Union["prefect.schedules.Schedule", List[str], List[WorkflowConfig]]],
]:
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
        context=dict(workflow_storage=workflow_storage)
    )
    sensor_config = sensor_schema.load(sensor_spec)

    return workflow_storage, sensor_config
