# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Defines 'available_dates_sensor' prefect flow.
"""

import pendulum
import prefect
import warnings
from get_secret_or_env_var import environ
from prefect import Flow, Parameter, task, unmapped
from prefect.engine import signals
from prefect.schedules import CronSchedule
from prefect.triggers import all_successful, any_failed
from typing import Any, Dict, List, NamedTuple, NoReturn, Optional, Sequence, Tuple

import flowclient

from .model import RunState, WorkflowRuns
from .utils import (
    dates_are_available,
    get_session,
    stencil_to_date_pairs,
    stencil_type_alias,
)


class WorkflowConfig(NamedTuple):
    """
    A namedtuple containing a workflow and its parameters.

    Attributes
    ----------
    workflow : prefect.Flow
        Prefect flow.
    parameters : dict
        Dict of parameters with which the workflow should be run.
    earliest_date : date
        Earliest date for which the workflow should run.
    date_stencil : list of datetime.date, int and/or pairs of date/int; optional
        List of elements defining dates or date intervals required by the workflow.
        Each element can be:
            - a date object corresponding to an absolute date,
            - an int corresponding to an offset (in days) relative to a reference date,
            - a length-2 list [start, end] of dates or offsets, corresponding to a
              date interval (inclusive of both limits).
    """

    workflow: Flow
    parameters: Dict[str, Any]
    earliest_date: Optional["datetime.date"] = None
    date_stencil: Optional[stencil_type_alias] = None


# Tasks -----------------------------------------------------------------------


@task
def get_available_dates(
    cdr_types: Optional[Sequence[str]] = None
) -> List[pendulum.Date]:
    """
    Task to return a union of the dates for which data is available in FlowDB for the specified set of CDR types.
    
    Parameters
    ----------
    cdr_types : list of str, optional
        Subset of CDR types for which to find available dates.
        If not provided, the union of available dates for all CDR types will be returned.
    
    Returns
    -------
    list of pendulum.Date
        List of available dates, in chronological order
    """
    prefect.context.logger.info(
        f"Getting available dates from FlowAPI at '{prefect.config.flowapi_url}'."
    )
    conn = flowclient.connect(
        url=prefect.config.flowapi_url, token=environ["FLOWAPI_TOKEN"]
    )
    dates = flowclient.get_available_dates(connection=conn)
    if cdr_types is None:
        prefect.context.logger.debug(
            "No CDR types provided. Will return available dates for all CDR types."
        )
        cdr_types = dates.keys()
    else:
        prefect.context.logger.debug(
            f"Returning available dates for CDR types {cdr_types}."
        )
        unknown_cdr_types = set(cdr_types).difference(dates.keys())
        if unknown_cdr_types:
            warnings.warn(f"No data available for CDR types {unknown_cdr_types}.")
    dates_union = set.union(
        *[
            set(pendulum.parse(date, exact=True) for date in dates[cdr_type])
            for cdr_type in cdr_types
            if cdr_type in dates.keys()
        ]
    )
    return sorted(list(dates_union))


@task
def filter_dates(
    available_dates: Sequence["datetime.date"], workflow_config: WorkflowConfig
) -> List["datetime.date"]:
    """
    Filter task to return only dates later than or equal to
    workflow_config.earliest_date, for which all dates represented by
    workflow_config.date_stencil are available.
    If workflow_config.earliest_date is None, date stencil availability will be
    checked for all available dates.
    If no workflow_config.date_stencil is None, dates will not be filtered by
    date stencil (this is equivalent to 'date_stencil=[0]').
    
    Parameters
    ----------
    available_dates : list of date
        List of dates to filter
    workflow_config : WorkflowConfig
        Workflow config with attributes 'earliest_date' and 'date_stencil'
    
    Returns
    -------
    list of date
        Filtered list of dates
    """
    prefect.context.logger.info(f"Filtering list of available dates.")
    filtered_dates = list(available_dates)

    if workflow_config.earliest_date is None:
        prefect.context.logger.debug("No earliest date provided.")
    else:
        prefect.context.logger.debug(
            f"Filtering out dates earlier than {workflow_config.earliest_date}."
        )
        filtered_dates = [
            date for date in filtered_dates if date >= workflow_config.earliest_date
        ]

    if workflow_config.date_stencil is None:
        prefect.context.logger.debug("No date stencil provided.")
    else:
        prefect.context.logger.debug(
            f"Returning reference dates for which all dates in stencil {workflow_config.date_stencil} are available."
        )
        filtered_dates = [
            date
            for date in filtered_dates
            if dates_are_available(workflow_config.date_stencil, date, available_dates)
        ]

    return filtered_dates


@task
def add_dates_to_parameters(
    workflow_configs: List[WorkflowConfig], lists_of_dates: List[List["datetime.date"]]
) -> List[Tuple[Flow, Dict[str, Any]]]:
    """
    For each workflow in a list of workflow configs, for each date in the
    corresponding list of dates, return a tuple (workflow, parameters) with
    parameters 'reference_date' and 'date_ranges' added to the parameters dict.

    Parameters
    ----------
    workflow_configs : list of WorkflowConfig
        List of workflow configs
    lists_of_dates : list of list of date
        List containing a list of dates for each workflow in workflow_configs

    Returns
    -------
    list of tuple (Flow, dict)
        List of tuples, each containing a prefect Flow and the parameters with which it should be run.
    """
    # Note: This is a 'reduce' task (i.e. it is not mapped over workflow configs).
    # This is because otherwise, tasks downstream of this one would need to be double-mapped
    # (once over workflow configs, and then over dates), and this is not possible with Prefect.
    prefect.context.logger.info(
        "Adding parameters 'reference_date' and 'date_ranges' to workflow parameters."
    )
    return [
        (
            workflow_config.workflow,
            dict(
                workflow_config.parameters,
                reference_date=date,
                date_ranges=stencil_to_date_pairs(
                    stencil=workflow_config.date_stencil or [0], reference_date=date
                ),
            ),
        )
        for workflow_config, dates in zip(workflow_configs, lists_of_dates)
        for date in dates
    ]


@task
def skip_if_already_run(parametrised_workflow: Tuple[Flow, Dict[str, Any]]) -> None:
    """
    Task to raise a SKIP signal if a workflow is already running or has previously run successfully
    with the given parameters.

    Parameters
    ----------
    parametrised_workflow : tuple (prefect.Flow, dict)
        Workflow, and associated parameters, for which previous runs should be checked

    Raises
    ------
    prefect.engine.signals.SKIP
        if this workflow with these parameters has already run successfully
    """
    workflow, parameters = parametrised_workflow
    prefect.context.logger.info(
        f"Checking whether workflow '{workflow.name}' has already run successfully with parameters {parameters}."
    )
    session = get_session(prefect.config.db_uri)
    state = WorkflowRuns.get_most_recent_state(
        workflow_name=workflow.name, parameters=parameters, session=session
    )
    session.close()

    if state is None:
        prefect.context.logger.debug(
            f"Workflow '{workflow.name}' has not previously run with parameters {parameters}."
        )
    elif state == RunState.failed:
        prefect.context.logger.debug(
            f"Workflow '{workflow.name}' previously failed with parameters {parameters}."
        )
    elif state == RunState.running:
        raise signals.SKIP(
            f"Workflow '{workflow.name}' is already running with parameters {parameters}."
        )
    elif state == RunState.success:
        raise signals.SKIP(
            f"Workflow '{workflow.name}' previously ran successfully with parameters {parameters}."
        )
    else:
        # This should never happen
        raise ValueError(f"Unrecognised workflow state: '{state}'")


@task
def record_workflow_run_state(
    parametrised_workflow: Tuple[Flow, Dict[str, Any]], state: RunState
) -> None:
    """
    Add a row to the database to record the state of a workflow run.

    Parameters
    ----------
    parametrised_workflow : tuple (prefect.Flow, dict)
        Workflow, and associated parameters, for which to record state.
    state : RunState
        Workflow run state.
    """
    workflow, parameters = parametrised_workflow
    prefect.context.logger.debug(
        f"Recording workflow '{workflow.name}' with parameters {parameters} as '{state.name}'."
    )
    session = get_session(prefect.config.db_uri)
    WorkflowRuns.set_state(
        workflow_name=workflow.name, parameters=parameters, state=state, session=session
    )
    session.close()


@task
def run_workflow(parametrised_workflow: Tuple[Flow, Dict[str, Any]]) -> None:
    """
    Run a workflow.

    Parameters
    ----------
    parametrised_workflow : tuple (prefect.Flow, dict)
        Workflow to run, and parameters to run it with.
    
    Notes
    -----
    
    The workflow will run once, starting immediately. If the workflow has a
    schedule, the schedule will be ignored.
    """
    workflow, parameters = parametrised_workflow
    prefect.context.logger.info(
        f"Running workflow '{workflow.name}' with parameters {parameters}."
    )
    state = workflow.run(parameters=parameters, run_on_schedule=False)
    if state.is_successful():
        prefect.context.logger.info(
            f"Workflow '{workflow.name}' ran successfully with parameters {parameters}."
        )
    else:
        raise signals.FAIL(
            f"Workflow '{workflow.name}' failed when run with parameters {parameters}."
        )


# Flows -----------------------------------------------------------------------


with Flow(name="Available dates sensor") as available_dates_sensor:
    # TODO: Read workflow configs from a db table, so that new workflows could be added while the date sensor is running.
    workflow_configs = Parameter("workflow_configs")
    available_dates = get_available_dates(
        cdr_types=Parameter("cdr_types", required=False)
    )
    filtered_dates = filter_dates.map(
        available_dates=unmapped(available_dates), workflow_config=workflow_configs
    )
    parametrised_workflows = add_dates_to_parameters(
        workflow_configs=workflow_configs, lists_of_dates=filtered_dates
    )

    running = record_workflow_run_state.map(
        parametrised_workflow=parametrised_workflows,
        state=unmapped(RunState.running),
        upstream_tasks=[
            skip_if_already_run.map(parametrised_workflow=parametrised_workflows)
        ],
    )
    workflow_runs = run_workflow.map(
        parametrised_workflow=parametrised_workflows, upstream_tasks=[running]
    )
    success = record_workflow_run_state.map(
        parametrised_workflow=parametrised_workflows,
        state=unmapped(RunState.success),
        upstream_tasks=[workflow_runs],
        task_args=dict(trigger=all_successful),
    )
    failed = record_workflow_run_state.map(
        parametrised_workflow=parametrised_workflows,
        state=unmapped(RunState.failed),
        upstream_tasks=[workflow_runs],
        task_args=dict(trigger=any_failed),
    )

available_dates_sensor.set_reference_tasks([success])
