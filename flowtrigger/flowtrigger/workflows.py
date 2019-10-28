# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Contains functions for constructing and running Prefect workflows.
"""

from prefect import Flow, Parameter, unmapped
from prefect.schedules import CronSchedule
from typing import List, Set, Dict, Tuple, Any, NoReturn

from . import tasks
from .utils import get_additional_parameter_names_for_notebooks, sort_notebook_labels


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
    notebooks: Dict[str, Dict[str, Any]],
    tag: "prefect.Task",
    parameter_tasks: Dict[str, "prefect.Task"],
) -> Tuple[List["prefect.Task"], List["prefect.Task"]]:
    """
    Add a series of tasks that execute notebooks, and tasks that convert notebooks to outputs.

    Parameters
    ----------
    notebooks : dict
        Dictionary of dictionaries describing notebook tasks.
        Each should have keys 'filename' and 'parameters', and optionally 'output'.
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

    sorted_notebook_labels = sort_notebook_labels(notebooks)

    for label in sorted_notebook_labels:
        parameter_tasks[label] = tasks.papermill_execute_notebook(
            input_filename=notebooks[label]["filename"],
            output_tag=tag,
            parameters={
                key: (parameter_tasks[value])
                for key, value in notebooks[label]["parameters"].items()
            },
        )
        notebook_tasks.append(parameter_tasks[label])
        if "output" in notebooks[label]:
            # Create PDF report from notebook
            output_tasks.append(
                tasks.convert_notebook_to_pdf(
                    notebook_path=parameter_tasks[label],
                    asciidoc_template=notebooks[label]["output"]["template"],
                )
            )

    return notebook_tasks, output_tasks


def add_mapped_notebooks_tasks(
    notebooks: Dict[str, Dict[str, Any]],
    tag: "prefect.Task",
    parameter_tasks: Dict[str, "prefect.Task"],
    mappable_parameter_names: Set[str],
) -> Tuple[List["prefect.Task"], List["prefect.Task"]]:
    """
    Add a series of tasks that execute notebooks, and tasks that convert notebooks to outputs.
    Unlike in 'add_notebooks_tasks', these tasks will be mapped over input parameters.

    Parameters
    ----------
    notebooks : dict
        Dictionary of dictionaries describing notebook tasks.
        Each should have keys 'filename' and 'parameters', and optionally 'output'.
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

    sorted_notebook_labels = sort_notebook_labels(notebooks)

    for label in sorted_notebook_labels:
        parameter_tasks[label] = tasks.papermill_execute_notebook.map(
            input_filename=unmapped(notebooks[label]["filename"]),
            output_tag=tag,
            parameters=tasks.mappable_dict.map(
                **{
                    key: (
                        parameter_tasks[value]
                        if value in mappable_parameter_names
                        else unmapped(parameter_tasks[value])
                    )
                    for key, value in notebooks[label]["parameters"].items()
                }
            ),
        )
        notebook_tasks.append(parameter_tasks[label])
        if "output" in notebooks[label]:
            # Create PDF report from notebook
            output_tasks.append(
                tasks.convert_notebook_to_pdf.map(
                    notebook_path=parameter_tasks[label],
                    asciidoc_template=unmapped(notebooks[label]["output"]["template"]),
                )
            )

    return notebook_tasks, output_tasks


def make_scheduled_notebooks_workflow(
    name: str, schedule: str, notebooks: Dict[str, Dict[str, Any]]
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
    notebooks : dict
        Dictionary of dictionaries describing notebook tasks.
        Each should have keys 'filename' and 'parameters', and optionally 'output'.
    
    Returns
    -------
    Flow
        Prefect workflow
    """
    # Create schedule
    flow_schedule = CronSchedule(schedule)

    # Get parameter names
    additional_parameter_names_for_notebooks = get_additional_parameter_names_for_notebooks(
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
    name: str, schedule: str, notebooks: Dict[str, Dict[str, Any]]
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
    notebooks : dict
        Dictionary of dictionaries describing notebook tasks.
        Each should have keys 'filename' and 'parameters', and optionally 'output'.
    
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
    additional_parameter_names_for_notebooks = get_additional_parameter_names_for_notebooks(
        notebooks=notebooks,
        reserved_parameter_names=date_filter_parameter_names.union(
            {"flowapi_url", "reference_date", "date_ranges"}
        ),
    )
    # Parameters that should be mapped over in workflow
    mappable_parameter_names = {"reference_date", "date_ranges"}.union(notebooks.keys())

    # Define workflow
    with Flow(name=name, schedule=flow_schedule) as workflow:
        # Parameters
        parameter_tasks = {
            pname: Parameter(pname, required=False)
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
    """
    Create a prefect workflow.

    Parameters
    ----------
    kind : str
        Kind of workflow to create (one of {'scheduled_notebooks', 'date_triggered_notebooks'}).
    **kwargs
        Arguments passed to the appropriate workflow maker.
    
    Returns
    -------
    Flow
        Prefect workflow
    """
    if kind == "scheduled_notebooks":
        return make_scheduled_notebooks_workflow(**kwargs)
    elif kind == "date_triggered_notebooks":
        return make_date_triggered_notebooks_workflow(**kwargs)
    else:
        raise ValueError(
            f"Unrecognised workflow kind: '{kind}'. "
            "Expected 'scheduled_notebooks' or 'date_triggered_notebooks'."
        )


def run_workflows(
    workflows: List[Flow], parameters: Dict[str, List[Dict[str, Any]]]
) -> NoReturn:
    """
    Run workflows with the provided sets of parameters.

    Parameters
    ----------
    workflows : list of Flow
        List of workflows to run
    parameters : dict
        Mapping from each workflow name to a list of parameter sets for which the workflow should run.
    
    Notes
    -----

    At the moment, this function will only run the first workflow in the list,
    with the first set of parameters for that workflow.
    """
    # TODO: workflow.run() is synchronous, so the second workflow will not run
    # until the first has finished (i.e. never, if the schedule has no end).
    # There should be a way around this using the lower-level FlowRunner class.
    for workflow in workflows:
        for params in parameters[workflow.name]:
            workflow.run(parameters=params)
