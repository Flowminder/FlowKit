# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from prefect import Flow, Parameter, unmapped, config
from prefect.schedules import CronSchedule
from pathlib import Path
from typing import List, Set, Dict, Tuple, Any
import yaml

from . import tasks
from .utils import get_parameter_names


def new_dates_subflow(
    *,
    cdr_types: "prefect.Task",
    earliest_date: "prefect.Task",
    date_stencil: "prefect.Task",
) -> "prefect.Task":
    """
    Assemble tasks to get a list of new dates for which notebooks should be run.

    Parameters
    ----------
    cdr_types : Task
        Task which returns a list of CDR types for which available dates should be found.
    earliest_date : Task
        Task which returns the earliest date that will pass the filter.
    date_stencil : Task
        Task which returns a date stencil to be used for filtering available dates.
    
    Returns
    -------
    Task
        Task which returns a list of dates.

    Notes
    -----

    This function must be called within a prefect Flow context.
    """
    all_dates = tasks.get_available_dates(cdr_types=cdr_types)
    dates_after_earliest = tasks.filter_dates_by_earliest_date(
        dates=all_dates, earliest_date=earliest_date
    )
    dates_with_available_stencil = tasks.filter_dates_by_stencil(
        dates=dates_after_earliest, available_dates=all_dates, date_stencil=date_stencil
    )
    new_dates = tasks.filter_dates_by_previous_runs(dates_with_available_stencil)
    return new_dates


def add_notebooks_tasks(
    notebooks: List[Dict[str, Any]],
    tag: "prefect.Task",
    parameter_tasks: Dict[str, "prefect.Task"],
) -> Tuple[List["prefect.Task"], List["prefect.Task"]]:
    """
    Add a series of tasks that execute notebooks, and tasks that convert notebooks to outputs.

    Parameters
    ----------
    notebooks : list of dict
        List of dictionaries describing notebook tasks.
        Each should have keys 'label', 'filename', 'parameters', and optionally 'output'.
    tag : Task
        Tag to append to output filenames.
    parameter_tasks : dict
        Mapping from parameter names to prefect tasks.
        Notebook execution tasks will be added to this dict.
    
    Returns
    -------
    notebook_tasks : list of Task
        list of prefect tasks that execute notebooks
    output_tasks : list of Task
        List of prefect tasks that create outputs from notebooks.
    
    Notes
    -----

    This function must be called within a prefect Flow context.
    """
    notebook_tasks = []
    output_tasks = []
    for notebook in notebooks:
        parameter_tasks[notebook["label"]] = tasks.papermill_execute_notebook(
            input_filename=notebook["filename"],
            output_tag=tag,
            parameters={
                key: (parameter_tasks[value])
                for key, value in notebook["parameters"].items()
            },
        )
        notebook_tasks.append(parameter_tasks[notebook["label"]])
        if "output" in notebook:
            # Create PDF report from notebook
            output_tasks.append(
                tasks.convert_notebook_to_pdf(
                    notebook_path=parameter_tasks[notebook["label"]],
                    asciidoc_template=notebook["output"]["template"],
                )
            )

    return notebook_tasks, output_tasks


def add_mapped_notebooks_tasks(
    notebooks: List[Dict[str, Any]],
    tag: "prefect.Task",
    parameter_tasks: Dict[str, "prefect.Task"],
    mappable_parameter_names: Set[str],
) -> Tuple[List["prefect.Task"], List["prefect.Task"]]:
    """
    Add a series of tasks that execute notebooks, and tasks that convert notebooks to outputs.
    Unlike in 'add_notebooks_tasks', these tasks will be mapped over input parameters.

    Parameters
    ----------
    notebooks : list of dict
        List of dictionaries describing notebook tasks.
        Each should have keys 'label', 'filename', 'parameters', and optionally 'output'.
    tag : Task
        Tag to append to output filenames. The output of this task will be mapped over.
    parameter_tasks : dict
        Mapping from parameter names to prefect tasks.
        Notebook execution tasks will be added to this dict.
    mappable_parameter_names : set of str
        Names of parameters which should be mapped over.
    
    Returns
    -------
    notebook_tasks : list of Task
        list of mapped prefect tasks that execute notebooks
    output_tasks : list of Task
        List of mapped prefect tasks that create outputs from notebooks.
    
    Notes
    -----

    This function must be called within a prefect Flow context.
    """
    notebook_tasks = []
    output_tasks = []
    for notebook in notebooks:
        parameter_tasks[notebook["label"]] = tasks.papermill_execute_notebook.map(
            input_filename=unmapped(notebook["filename"]),
            output_tag=tag,
            parameters=tasks.mappable_dict.map(
                **{
                    key: (
                        parameter_tasks[value]
                        if value in mappable_parameter_names
                        else unmapped(parameter_tasks[value])
                    )
                    for key, value in notebook["parameters"].items()
                }
            ),
        )
        notebook_tasks.append(parameter_tasks[notebook["label"]])
        if "output" in notebook:
            # Create PDF report from notebook
            output_tasks.append(
                tasks.convert_notebook_to_pdf.map(
                    notebook_path=parameter_tasks[notebook["label"]],
                    asciidoc_template=unmapped(notebook["output"]["template"]),
                )
            )

    return notebook_tasks, output_tasks


def make_scheduled_notebooks_workflow(
    name: str, schedule: str, notebooks: List[Dict[str, Any]]
) -> Flow:
    """
    Build a prefect workflow that runs a set of Jupyter notebooks on a schedule.
    The FlowAPI URL will be available to the notebooks as parameter 'flowapi_url'.

    Parameters
    ----------
    name : str
        Name for the workflow
    schedule : str
        Cron string describing the schedule on which the workflow will run the notebooks.
    notebooks : list of dict
        List of dictionaries describing notebook tasks.
        Each should have keys 'label', 'filename', 'parameters', and optionally 'output'.
    
    Returns
    -------
    Flow
        Prefect workflow
    """
    # Create schedule
    flow_schedule = CronSchedule(schedule)

    # Get parameter names
    notebook_labels, additional_parameter_names_for_notebooks = get_parameter_names(
        notebooks=notebooks, reserved_parameter_names={"flowapi_url"}
    )

    # Define workflow
    with Flow(name=name, schedule=flow_schedule) as workflow:
        # Parameters
        parameter_tasks = {
            pname: Parameter(pname)
            for pname in additional_parameter_names_for_notebooks
        }

        # Get FlowAPI URL so that it can be passed as a parameter to notebook execution tasks
        parameter_tasks["flowapi_url"] = tasks.get_flowapi_url()

        # Record workflow run as 'in_process'
        in_process = tasks.record_workflow_in_process()

        # Get unique tag for this workflow run
        tag = tasks.get_tag(upstream_tasks=[in_process])

        # Execute notebooks
        notebook_tasks, output_tasks = add_notebooks_tasks(
            notebooks=notebooks, tag=tag, parameter_tasks=parameter_tasks
        )

        # Record workflow run as 'done', if successful
        done = tasks.record_workflow_done(upstream_tasks=notebook_tasks + output_tasks)
        # Record workflow run as 'failed', if failed
        failed = tasks.record_workflow_failed(
            upstream_tasks=notebook_tasks + output_tasks
        )

    # Set tasks that define the workflow state
    workflow.set_reference_tasks([done])
    return workflow


def make_date_triggered_notebooks_workflow(
    name: str, schedule: str, notebooks: List[Dict[str, Any]]
) -> Flow:
    """
    Build a prefect workflow that runs a set of Jupyter notebooks for each new date
    of data available in FlowKit.
    In addition to parameters requested in the notebooks, this workflow will have
    the following parameters:
        - cdr_types:     A list of CDR types for which available dates will be
                         checked (default is all available CDR types).
        - earliest_date: The earliest date for which the notebooks should run.
        - date_stencil:  A list of dates (either absolute dates or offsets relative
                         to a reference date), or pairs of dates defining an interval.
                         For each available date, the availability of all dates
                         included in the stencil will be checked.
    For each date, the date itself and the date ranges corresponding to the stencil
    will be available to notebooks as parameters 'reference_date' and 'date_ranges',
    respectively. The FlowAPI URL will be available as parameter 'flowapi_url'.

    Parameters
    ----------
    name : str
        Name for the workflow
    schedule : str
        Cron string describing the schedule on which the workflow will check for new data.
    notebooks : list of dict
        List of dictionaries describing notebook tasks.
        Each should have keys 'label', 'filename', 'parameters', and optionally 'output'.
    
    Returns
    -------
    Flow
        Prefect workflow
    """
    # Create schedule
    flow_schedule = CronSchedule(schedule)

    # Get parameter names
    # Parameters required for date filter (will all have default None)
    date_filter_parameter_names = {"cdr_types", "earliest_date", "date_stencil"}
    # Parameters used in notebooks
    notebook_labels, additional_parameter_names_for_notebooks = get_parameter_names(
        notebooks=notebooks,
        reserved_parameter_names=date_filter_parameter_names.union(
            {"flowapi_url", "reference_date", "date_ranges"}
        ),
    )
    # Parameters that should be mapped over in workflow
    mappable_parameter_names = notebook_labels.union({"reference_date", "date_ranges"})

    # Define workflow
    with Flow(name=name, schedule=flow_schedule) as workflow:
        # Parameters
        parameter_tasks = {
            pname: Parameter(pname, default=None, required=False)
            for pname in date_filter_parameter_names
        }
        parameter_tasks.update(
            {
                pname: Parameter(pname)
                for pname in additional_parameter_names_for_notebooks
            }
        )

        # Get FlowAPI URL so that it can be passed as a parameter to notebook execution tasks
        parameter_tasks["flowapi_url"] = tasks.get_flowapi_url()

        # Get list of dates
        parameter_tasks["reference_date"] = new_dates_subflow(
            cdr_types=parameter_tasks["cdr_types"],
            earliest_date=parameter_tasks["earliest_date"],
            date_stencil=parameter_tasks["date_stencil"],
        )

        # Record each workflow run as 'in_process'
        in_process = tasks.record_workflow_in_process.map(
            reference_date=parameter_tasks["reference_date"]
        )

        # Get unique tag for each reference date
        tag = tasks.get_tag.map(
            reference_date=parameter_tasks["reference_date"],
            upstream_tasks=[in_process],
        )

        # Get date ranges for each reference date
        parameter_tasks["date_ranges"] = tasks.get_date_ranges.map(
            reference_date=parameter_tasks["reference_date"],
            date_stencil=unmapped(parameter_tasks["date_stencil"]),
        )

        # Execute notebooks for each reference date
        notebook_tasks, output_tasks = add_mapped_notebooks_tasks(
            notebooks=notebooks,
            tag=tag,
            parameter_tasks=parameter_tasks,
            mappable_parameter_names=mappable_parameter_names,
        )

        # Record each successful workflow run as 'done'
        done = tasks.record_workflow_done.map(
            reference_date=parameter_tasks["reference_date"],
            upstream_tasks=notebook_tasks + output_tasks,
        )

        # Record any unsuccessful workflow runs as 'failed'
        failed = tasks.record_any_failed_workflows(
            reference_dates=parameter_tasks["reference_date"],
            upstream_tasks=[in_process, done],
        )

    # Set tasks that define the workflow state
    workflow.set_reference_tasks([done, failed])
    return workflow


def make_workflow(kind: str, **kwargs):
    if kind == "date_triggered_notebooks":
        return make_date_triggered_notebooks_workflow(**kwargs)
    elif kind == "scheduled_notebooks":
        return make_scheduled_notebooks_workflow(**kwargs)
    else:
        raise ValueError(
            f"Unrecognised workflow kind: '{kind}'. "
            "Expected 'date_triggered_notebooks' or 'scheduled_notebooks'."
        )


def load_and_validate_workflows_yaml(filename: str) -> List[Dict[str, Any]]:
    # TODO: This would be neater and more robust as a marshmallow schema
    with open(filename, "r") as f:
        workflows_spec = yaml.safe_load(f)

    if not isinstance(workflows_spec, list):
        raise TypeError(
            "Workflows yaml file does not contain a sequence of workflow specifications."
        )
    workflow_names = []
    for workflow_spec in workflows_spec:
        if not isinstance(workflow_spec, dict):
            raise TypeError("Invalid workflow specification: not a mapping.")
        expected_keys = {"name", "kind", "schedule", "notebooks", "parameters"}
        missing_keys = expected_keys.difference(workflow_spec.keys())
        if missing_keys:
            raise KeyError(
                f"Workflow specification parameters {missing_keys} were not provided."
            )
        unexpected_keys = set(workflow_spec.keys()).difference(expected_keys)
        if unexpected_keys:
            raise KeyError(
                f"Received unexpected workflow specification parameters {unexpected_keys}."
            )
        # Note: validity of name, kind and schedule are checked when building the workflow
        if workflow_spec["name"] in workflow_names:
            raise ValueError(f"Duplicate workflow name: '{workflow_spec['name']}'.")
        else:
            workflow_names.append(workflow_spec["name"])
        if not isinstance(workflow_spec["notebooks"], list):
            raise TypeError(
                "Invalid workflow specification: 'notebooks' is not a sequence of notebook task specifications."
            )
        notebook_labels = []
        for notebook in workflow_spec["notebooks"]:
            required_keys = {"label", "filename", "parameters"}
            missing_keys = required_keys.difference(notebook.keys())
            if missing_keys:
                raise KeyError(
                    f"Notebook task specification missing required keys {missing_keys}."
                )
            unexpected_keys = (
                set(notebook.keys()).difference(required_keys).difference({"output"})
            )
            if unexpected_keys:
                raise KeyError(
                    f"Notebook task specification contains unexpected keys {unexpected_keys}."
                )
            if notebook["label"] in notebook_labels:
                raise ValueError(f"Duplicate notebook label: '{notebook['label']}'.")
            else:
                notebook_labels.append(notebook["label"])
            if not (Path(config.inputs.inputs_dir) / notebook["filename"]).exists():
                raise ValueError(f"Notebook file '{notebook['filename']}' not found.")
            if not isinstance(notebook["parameters"], dict):
                raise TypeError(
                    "Invalid notebook task specification: 'parameters' is not a mapping."
                )
            # Note: validity of parameter names is checked when building the workflow.
            if "output" in notebook:
                if isinstance(notebook["output"], dict):
                    if "template" not in notebook["output"]:
                        notebook["output"]["template"] = None
                    if (notebook["output"]["template"] is not None) and not (
                        Path(config.inputs.inputs_dir) / notebook["output"]["template"]
                    ).exists():
                        raise ValueError(
                            f"Template '{notebook['output']['template']}' not found."
                        )
                    unexpected_keys = set(notebook["output"].keys()).difference(
                        {"template"}
                    )
                    if unexpected_keys:
                        raise KeyError(
                            f"Unexpected keys in notebook output block: {unexpected_keys}."
                        )
                elif notebook["output"]:
                    notebook["output"] = dict(template=None)
                else:
                    notebook.pop("output")
        if not isinstance(workflow_spec["parameters"], list):
            raise TypeError(
                "Invalid workflow specification: 'parameters' is not a list of parameter mappings."
            )
        for params in workflow_spec["parameters"]:
            # Note: validity of the provided parameters cannot be checked until the workflow has been constructed.
            if not isinstance(params, dict):
                raise TypeError(
                    "Invalid workflow specification: 'parameters' is not a list of parameter mappings."
                )

    return workflows_spec


def parse_workflows_yaml(
    filename: str
) -> Tuple[List[Flow], Dict[str, List[Dict[str, Any]]]]:
    """
    Construct workflows defined in an input file.

    Parameters
    ----------
    filename : str
        Name of yaml input file
    
    Returns
    -------
    workflows : list of Flow
        List of prefect workflows
    run_parameters : dict
        mapping from workflow names to a list of dicts of parameters for which the workflow should be run
    """
    workflow_specs = load_and_validate_workflows_yaml(filename)

    workflows = []
    run_parameters = {}
    for workflow_spec in workflow_specs:
        parameters = workflow_spec.pop("parameters")
        workflow = make_workflow(**workflow_spec)
        workflows.append(workflow)
        # TODO: Store workflow in Local storage, instead of returning in a list?
        for params_dict in parameters:
            param_names = set(params_dict.keys())
            workflow_param_names = {p.name for p in workflow.parameters()}
            required_param_names = {p.name for p in workflow.parameters() if p.required}
            missing_params = required_param_names.difference(param_names)
            if missing_params:
                raise KeyError(
                    f"Missing required parameters {missing_params} for workflow '{workflow.name}'."
                )
            unexpected_params = param_names.difference(workflow_param_names)
            if unexpected_params:
                raise KeyError(
                    f"Unexpected parameters provided for workflow '{workflow.name}': {unexpected_params}."
                )
            # TODO: For date-triggered workflows, we should be able to validate the values for parameters 'cdr_types', 'earliest_date' and 'date_stencil'
        run_parameters[workflow_spec["name"]] = parameters

    return workflows, run_parameters
