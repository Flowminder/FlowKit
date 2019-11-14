# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Contains tasks and functions for constructing Prefect workflows.
"""

import papermill
import prefect
from pathlib import Path
from prefect import Flow, Parameter, task
from typing import Any, Dict, Optional, OrderedDict

from .utils import (
    asciidoc_to_pdf,
    get_additional_parameter_names_for_notebooks,
    get_output_filename,
    get_params_hash,
    make_json_serialisable,
    notebook_to_asciidoc,
)


# Tasks -----------------------------------------------------------------------


@task
def get_flowapi_url() -> str:
    """
    Task to return FlowAPI URL.

    Returns
    -------
    str
        FlowAPI URL
    """
    # This task is defined so that the flowapi url can be passed as a parameter
    # to notebook tasks.
    return prefect.config.flowapi_url


@task
def get_tag(reference_date: Optional["datetime.date"] = None) -> str:
    """
    Task to get a string to append to output filenames from this workflow run.
    The tag is unique for each set of workflow parameters.

    Parameters
    ----------
    reference_date : date, optional
        Reference date for which the workflow is running

    Returns
    -------
    str
        Tag for output filenames
    """
    params_hash = get_params_hash(prefect.context.parameters)
    ref_date_string = f"_{reference_date}" if reference_date is not None else ""
    return f"{prefect.context.flow_name}{ref_date_string}_{params_hash}"


@task
def papermill_execute_notebook(
    input_filename: str,
    output_tag: str,
    parameters: Optional[Dict[str, Any]] = None,
    **kwargs,
) -> str:
    """
    Task to execute a notebook using Papermill.
    
    Parameters
    ----------
    input_filename : str
        Filename of input notebook (assumed to be in the inputs directory)
    output_tag : str
        Tag to append to output filename
    parameters : dict, optional
        Parameters to pass to the notebook
    **kwargs
        Additional keyword arguments to pass to papermill.execute_notebook
    
    Returns
    -------
    str
        Path to executed notebook
    """
    # Papermill injects all parameters into the notebook metadata, which gets
    # json-serialised, so all parameters must be json serialisable
    # (see https://github.com/nteract/papermill/issues/412).
    # 'make_json_serialisable()' has the convenient side-effect of converting tuples to
    # lists, where we would otherwise have to register a custom papermill translator
    # for tuples.
    safe_params = make_json_serialisable(parameters)
    prefect.context.logger.info(
        f"Executing notebook '{input_filename}' with parameters {safe_params}."
    )

    output_filename = get_output_filename(input_filename=input_filename, tag=output_tag)
    input_path = str(Path(prefect.config.inputs.inputs_dir) / input_filename)
    output_path = str(Path(prefect.config.outputs.notebooks_dir) / output_filename)

    prefect.context.logger.debug(f"Output notebook will be '{output_path}'.")

    papermill.execute_notebook(
        input_path, output_path, parameters=safe_params, **kwargs
    )

    prefect.context.logger.info(f"Finished executing notebook.")

    return output_path


@task
def convert_notebook_to_pdf(
    notebook_path: str,
    output_filename: Optional[str] = None,
    asciidoc_template: Optional[str] = None,
) -> str:
    """
    Task to convert a notebook to PDF, via asciidoc (without executing the notebook).
    
    Parameters
    ----------
    notebook_path : str
        Path to notebook
    output_filename : str, optional
        Filename for output PDF file.
        If not provided, this will be the name of the input notebook with the extension changed to '.pdf'
    asciidoc_template : str, optional
        Filename of a non-default template to use when exporting to asciidoc
        (assumed to be in the inputs directory)
    
    Returns
    -------
    str
        Path to output PDF file
    """
    prefect.context.logger.info(f"Converting notebook '{notebook_path}' to PDF.")
    if output_filename is None:
        output_filename = f"{Path(notebook_path).stem}.pdf"
    output_path = str(Path(prefect.config.outputs.reports_dir) / output_filename)

    if asciidoc_template is None:
        if "asciidoc_template_path" in prefect.config:
            asciidoc_template_path = prefect.config.asciidoc_template_path
        else:
            # If no template is provided, and no default template is set in the config,
            # run nbconvert without specifying a template (i.e. use the default nbconvert asciidoc template).
            asciidoc_template_path = None
    else:
        asciidoc_template_path = str(
            Path(prefect.config.inputs.inputs_dir) / asciidoc_template
        )
    prefect.context.logger.debug(
        f"Using template '{asciidoc_template_path}' to convert notebook to asciidoc."
    )

    body, resources = notebook_to_asciidoc(notebook_path, asciidoc_template_path)

    prefect.context.logger.debug("Converted notebook to asciidoc.")
    prefect.context.logger.debug("Converting asciidoc to PDF...")

    asciidoc_to_pdf(body, resources, output_path)

    prefect.context.logger.info(f"Created report '{output_filename}'.")

    return output_path


# Flows -----------------------------------------------------------------------


def make_notebooks_workflow(
    name: str, notebooks: OrderedDict[str, Dict[str, Any]]
) -> Flow:
    """
    Build a prefect flow that runs a set of interdependent Jupyter notebooks.
    The FlowAPI URL will be available to the notebooks as parameter 'flowapi_url'.

    Parameters
    ----------
    name : str
        Name for the workflow
    notebooks : OrderedDict
        Ordered dictionary of dictionaries describing notebook tasks.
        Each should have keys 'filename' and 'parameters', and optionally 'output'.
    
    Returns
    -------
    Flow
        Workflow that runs the notebooks.
    """
    # Get parameter names
    parameter_names = get_additional_parameter_names_for_notebooks(
        notebooks=notebooks, reserved_parameter_names={"flowapi_url"}
    )

    # Define workflow
    with Flow(name=name, schedule=flow_schedule) as workflow:
        # Parameters
        parameter_tasks = {
            pname: Parameter(pname)
            for pname in parameter_names.union("reference_date", "date_ranges")
        }
        # Instantiating a Parameter doesn't add it to the flow. The available dates sensor
        # will always pass the "date_ranges" parameter, so we need to explicitly add this
        # parameter in case it's not used by any of the notebook tasks
        # (https://github.com/PrefectHQ/prefect/issues/1541)
        workflow.add_task(parameter_tasks["date_ranges"])

        # Get FlowAPI URL so that it can be passed as a parameter to notebook execution tasks
        parameter_tasks["flowapi_url"] = get_flowapi_url()

        # Get unique tag for this workflow run
        tag = get_tag(reference_date=parameter_tasks["reference_date"])

        # Execute notebooks
        for key, notebook in notebooks.items():
            parameter_tasks[key] = papermill_execute_notebook(
                input_filename=notebook["filename"],
                output_tag=tag,
                parameters={
                    k: parameter_tasks[v] for k, v in notebook["parameters"].items()
                },
            )
            if "output" in notebook:
                # Create PDF report from notebook
                convert_notebook_to_pdf(
                    notebook_path=parameter_tasks[key],
                    asciidoc_template=notebook["output"]["template"],
                )

    return workflow


# def add_mapped_notebooks_tasks(
#     notebooks: Dict[str, Dict[str, Any]],
#     tag: "prefect.Task",
#     parameter_tasks: Dict[str, "prefect.Task"],
#     mappable_parameter_names: Set[str],
# ) -> Tuple[List["prefect.Task"], List["prefect.Task"]]:
#     """
#     Add a series of tasks that execute notebooks, and tasks that convert notebooks to outputs.
#     Unlike in 'add_notebooks_tasks', these tasks will be mapped over input parameters.

#     Parameters
#     ----------
#     notebooks : dict
#         Dictionary of dictionaries describing notebook tasks.
#         Each should have keys 'filename' and 'parameters', and optionally 'output'.
#     tag : Task
#         Tag to append to output filenames. The output of this task will be mapped over.
#     parameter_tasks : dict
#         Mapping from parameter names to prefect tasks.
#         Notebook execution tasks will be added to this dict.
#     mappable_parameter_names : set of str
#         Names of parameters which should be mapped over.

#     Returns
#     -------
#     notebook_tasks : list of Task
#         list of mapped prefect tasks that execute notebooks
#     output_tasks : list of Task
#         List of mapped prefect tasks that create outputs from notebooks.

#     Notes
#     -----

#     This function must be called within a prefect Flow context.
#     """
#     # TODO: This function is almost identical to 'add_notebooks_tasks', except
#     # that here all tasks are mapped. It may make more sense to restructure the
#     # date_triggered_notebooks workflow into a date_trigger workflow that
#     # triggers individual notebooks workflows. Each individual notebooks
#     # workflow could then be created using the same logic as
#     # 'make_scheduled_notebooks_workflow', and no mapping would be required
#     # outside of the date_trigger workflow.
#     notebook_tasks = []
#     output_tasks = []

#     sorted_notebook_labels = sort_notebook_labels(notebooks)

#     for label in sorted_notebook_labels:
#         parameter_tasks[label] = tasks.papermill_execute_notebook.map(
#             input_filename=unmapped(notebooks[label]["filename"]),
#             output_tag=tag,
#             parameters=tasks.mappable_dict.map(
#                 **{
#                     key: (
#                         parameter_tasks[value]
#                         if value in mappable_parameter_names
#                         else unmapped(parameter_tasks[value])
#                     )
#                     for key, value in notebooks[label]["parameters"].items()
#                 }
#             ),
#         )
#         notebook_tasks.append(parameter_tasks[label])
#         if "output" in notebooks[label]:
#             # Create PDF report from notebook
#             output_tasks.append(
#                 tasks.convert_notebook_to_pdf.map(
#                     notebook_path=parameter_tasks[label],
#                     asciidoc_template=unmapped(notebooks[label]["output"]["template"]),
#                 )
#             )

#     return notebook_tasks, output_tasks


# def make_date_triggered_notebooks_workflow(
#     name: str, schedule: str, notebooks: Dict[str, Dict[str, Any]]
# ) -> Flow:
#     """
#     Build a prefect workflow that runs a set of Jupyter notebooks for each new date
#     of data available in FlowKit.
#     In addition to parameters requested in the notebooks, this workflow will have
#     the following parameters:
#         - cdr_types:     A list of CDR types for which available dates will be
#                          checked (default is all available CDR types).
#         - earliest_date: The earliest date for which the notebooks should run.
#         - date_stencil:  A list of dates (either absolute dates or offsets relative
#                          to a reference date), or pairs of dates defining an interval.
#                          For each available date, the availability of all dates
#                          included in the stencil will be checked.
#     For each date, the date itself and the date ranges corresponding to the stencil
#     will be available to notebooks as parameters 'reference_date' and 'date_ranges',
#     respectively. The FlowAPI URL will be available as parameter 'flowapi_url'.

#     Parameters
#     ----------
#     name : str
#         Name for the workflow
#     schedule : str
#         Cron string describing the schedule on which the workflow will check for new data.
#     notebooks : dict
#         Dictionary of dictionaries describing notebook tasks.
#         Each should have keys 'filename' and 'parameters', and optionally 'output'.

#     Returns
#     -------
#     Flow
#         Prefect workflow
#     """
#     # Create schedule
#     flow_schedule = CronSchedule(schedule)

#     # Get parameter names
#     # Parameters required for date filter (will all have default None)
#     date_filter_parameter_names = {"cdr_types", "earliest_date", "date_stencil"}
#     # Parameters used in notebooks
#     additional_parameter_names_for_notebooks = get_additional_parameter_names_for_notebooks(
#         notebooks=notebooks,
#         reserved_parameter_names=date_filter_parameter_names.union(
#             {"flowapi_url", "reference_date", "date_ranges"}
#         ),
#     )
#     # Parameters that should be mapped over in workflow
#     mappable_parameter_names = {"reference_date", "date_ranges"}.union(notebooks.keys())

#     # Define workflow
#     with Flow(name=name, schedule=flow_schedule) as workflow:
#         # Parameters
#         parameter_tasks = {
#             pname: Parameter(pname, required=False)
#             for pname in date_filter_parameter_names
#         }
#         parameter_tasks.update(
#             {
#                 pname: Parameter(pname)
#                 for pname in additional_parameter_names_for_notebooks
#             }
#         )

#         # Get FlowAPI URL so that it can be passed as a parameter to notebook execution tasks
#         parameter_tasks["flowapi_url"] = tasks.get_flowapi_url()

#         # Get list of dates
#         parameter_tasks["reference_date"] = new_dates_subflow(
#             cdr_types=parameter_tasks["cdr_types"],
#             earliest_date=parameter_tasks["earliest_date"],
#             date_stencil=parameter_tasks["date_stencil"],
#         )

#         # Record each workflow run as 'in_process'
#         in_process = tasks.record_workflow_in_process.map(
#             reference_date=parameter_tasks["reference_date"]
#         )

#         # Get unique tag for each reference date
#         tag = tasks.get_tag.map(
#             reference_date=parameter_tasks["reference_date"],
#             upstream_tasks=[in_process],
#         )

#         # Get date ranges for each reference date
#         parameter_tasks["date_ranges"] = tasks.get_date_ranges.map(
#             reference_date=parameter_tasks["reference_date"],
#             date_stencil=unmapped(parameter_tasks["date_stencil"]),
#         )

#         # Execute notebooks for each reference date
#         notebook_tasks, output_tasks = add_mapped_notebooks_tasks(
#             notebooks=notebooks,
#             tag=tag,
#             parameter_tasks=parameter_tasks,
#             mappable_parameter_names=mappable_parameter_names,
#         )

#         # Record each successful workflow run as 'done'
#         done = tasks.record_workflow_done.map(
#             reference_date=parameter_tasks["reference_date"],
#             upstream_tasks=notebook_tasks + output_tasks,
#         )

#         # Record any unsuccessful workflow runs as 'failed'
#         failed = tasks.record_any_failed_workflows(
#             reference_dates=parameter_tasks["reference_date"],
#             upstream_tasks=[in_process, done],
#         )

#     # Set tasks that define the workflow state
#     workflow.set_reference_tasks([done, failed])
#     return workflow


# def make_workflow(kind: str, **kwargs):
#     """
#     Create a prefect workflow.

#     Parameters
#     ----------
#     kind : str
#         Kind of workflow to create (one of {'scheduled_notebooks', 'date_triggered_notebooks'}).
#     **kwargs
#         Arguments passed to the appropriate workflow maker.

#     Returns
#     -------
#     Flow
#         Prefect workflow
#     """
#     if kind == "scheduled_notebooks":
#         return make_scheduled_notebooks_workflow(**kwargs)
#     elif kind == "date_triggered_notebooks":
#         return make_date_triggered_notebooks_workflow(**kwargs)
#     else:
#         raise ValueError(
#             f"Unrecognised workflow kind: '{kind}'. "
#             "Expected 'scheduled_notebooks' or 'date_triggered_notebooks'."
#         )


# def run_workflows(
#     workflows: List[Flow],
#     parameters: Dict[str, List[Dict[str, Any]]],
#     run_on_schedule: bool = True,
# ) -> None:
#     """
#     Run workflows with the provided sets of parameters.

#     Parameters
#     ----------
#     workflows : list of Flow
#         List of workflows to run
#     parameters : dict
#         Mapping from each workflow name to a list of parameter sets for which the workflow should run.
#     run_on_schedule : bool, default True
#         Set to False to ignore schedules and run each workflow only once.

#     Notes
#     -----

#     At the moment, this function will only run the first workflow in the list,
#     with the first set of parameters for that workflow.
#     """
#     # TODO: workflow.run() is synchronous, so the second workflow will not run
#     # until the first has finished (i.e. never, if the schedule has no end).
#     # There should be a way around this using the lower-level FlowRunner class.
#     for workflow in workflows:
#         for params in parameters[workflow.name]:
#             workflow.run(parameters=params, run_on_schedule=run_on_schedule)
