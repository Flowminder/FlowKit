# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from prefect import Flow, Parameter, unmapped, config
from prefect.schedules import CronSchedule
from pathlib import Path
from datetime import timedelta
from typing import List, Set, Dict, Tuple, Any, Optional
import yaml

from . import tasks


def get_parameter_names(
    notebooks: List[Dict[str, Any]], reserved_parameter_names: Optional[Set[str]] = None
) -> Tuple[Set[str], Set[str], Set[str]]:
    """
    Extract sets of parameter names from a list of notebook task specifications.

    Parameters
    ----------
    notebooks : list of dict
        List of dictionaries describing notebook tasks.
    reserved_parameter_names : set of str, optional
        Names of parameters used within workflow, which cannot be used as notebook labels.
    
    Returns
    -------
    notebook_parameter_names : set of str
        Names of parameters passed to notebooks
    notebook_labels : set of str
        Notebook labels, which can be used as parameter names in other notebook tasks
    additional_parameter_names_for_notebooks : set of str
        Names of parameters used by notebooks which are not either reserved parameter names or notebook labels
    """
    if reserved_parameter_names is None:
        reserved_parameter_names = set()
    # Labels for notebook tasks
    notebook_labels = set(notebook["label"] for notebook in notebooks)
    forbidden_labels = notebook_labels.intersection(reserved_parameter_names)
    if forbidden_labels:
        raise ValueError(
            f"Notebook labels {forbidden_labels} are forbidden for this workflow. "
            f"Reserved parameter names are {reserved_parameter_names}."
        )
    # Parameters requested in notebooks
    notebook_parameter_names = set.union(
        *[set(notebook["parameters"].values()) for notebook in notebooks]
    )
    # Additional parameters required for notebooks
    additional_parameter_names_for_notebooks = notebook_parameter_names.difference(
        reserved_parameter_names
    ).difference(notebook_labels)
    return (
        notebook_parameter_names,
        notebook_labels,
        additional_parameter_names_for_notebooks,
    )


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


def add_mapped_notebooks_tasks(
    notebooks: List[Dict[str, Any]],
    tag: List["prefect.Task"],
    parameter_tasks: Dict[str, "prefect.Task"],
    output_tasks: List["prefect.Task"],
    mappable_parameter_names: Set[str],
) -> None:
    """
    Add a series of tasks to execute notebooks, and optionally produce PDF outputs.

    Parameters
    ----------
    notebooks : list of dict
        List of dictionaries describing notebook tasks.
        Each should have keys 'label', 'filename', 'parameters', and optionally 'output'.
    tag : list of Task
        Tags to append to output filenames. This list will be mapped over.
    parameter_tasks : dict
        Mapping from parameter names to prefect tasks.
        Notebook execution tasks will be added to this dict.
    output_tasks : list of Task
        List of prefect tasks that create outputs.
        PDF output tasks will be appended to this list.
    mappable_parameter_names : set of str
        Names of parameters which should be mapped over.
    
    Notes
    -----

    This function must be called within a prefect Flow context.
    """
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
        # TODO: on parsing input file, remove "output" key if no output requested, and make it a dict otherwise (with key "template", default None)
        if "output" in notebook:
            # Create PDF report from notebook
            output_tasks.append(
                tasks.convert_notebook_to_pdf.map(
                    notebook_path=parameter_tasks[notebook["label"]],
                    asciidoc_template=unmapped(notebook["output"]["template"]),
                )
            )


def make_scheduled_notebooks_workflow(
    name: str, schedule: str, notebooks: List[Dict[str, Any]]
) -> Flow:
    raise NotImplementedError("Scheduled notebooks workflow maker not implemented yet.")


def make_date_triggered_notebooks_workflow(
    name: str, schedule: str, notebooks: List[Dict[str, Any]]
) -> Flow:
    # Create schedule
    flow_schedule = CronSchedule(schedule)

    # Get parameter names
    # Parameters required for date filter (will all have default None)
    date_filter_parameter_names = {"cdr_types", "earliest_date", "date_stencil"}
    notebook_parameter_names, notebook_labels, additional_parameter_names_for_notebooks = get_parameter_names(
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

        # Get FlowAPI URL so that it can be passed as a parameter to notebook execution tasks, if required
        if "flowapi_url" in notebook_parameter_names:
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

        # Get date ranges for each reference date, if required
        if "date_ranges" in notebook_parameter_names:
            parameter_tasks["date_ranges"] = tasks.get_date_ranges.map(
                reference_date=parameter_tasks["reference_date"],
                date_stencil=unmapped(parameter_tasks["date_stencil"]),
            )

        output_tasks = []
        # Run notebooks for each reference date
        add_mapped_notebooks_tasks(
            notebooks=notebooks,
            tag=tag,
            parameter_tasks=parameter_tasks,
            output_tasks=output_tasks,
            mappable_parameter_names=mappable_parameter_names,
        )

        # Record each successful workflow run as 'done'
        done = tasks.record_workflow_done.map(
            reference_date=parameter_tasks["reference_date"],
            upstream_tasks=[parameter_tasks[nblabel] for nblabel in notebook_labels]
            + output_tasks,
        )

        # Record any unsuccessful workflow runs as 'failed'
        failed = tasks.record_any_failed_workflows(
            reference_dates=parameter_tasks["reference_date"],
            upstream_tasks=[in_process, done],
        )

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
    # TODO: This would be neater and more robust with marshmallow
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
