# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from prefect import task, context
from prefect.triggers import all_successful, any_failed
import papermill as pm
from typing import Optional, Dict, Sequence, List, Tuple, Union, Set, Any
import nbformat
from nbconvert import ASCIIDocExporter
from tempfile import TemporaryDirectory
from sh import asciidoctor_pdf
from pathlib import Path
import pendulum
import json
from hashlib import md5
from get_secret_or_env_var import environ

import flowclient

from .utils import (
    get_output_filename,
    get_params_hash,
    dates_are_available,
    stencil_type_alias,
    get_session,
    stencil_to_date_pairs,
    make_json_serialisable,
)
from .model import workflow_runs


@task
def get_label(reference_date: "datetime.date") -> str:
    """
    Task to get a string to append to output filenames from this workflow run.
    The label is unique for each set of workflow parameters and reference date.

    Parameters
    ----------
    reference_date : date
        Reference date for which the workflow is running

    Returns
    -------
    str
        Label for output filenames
    """
    params_hash = get_params_hash(context.parameters)
    return f"__{context.flow_name}_{params_hash}_{reference_date}"


@task
def get_date_ranges(
    reference_date: "datetime.date", date_stencil: Optional[stencil_type_alias] = None
) -> List[Tuple[pendulum.Date, pendulum.Date]]:
    """
    Task to get a list of date pairs from a date stencil.

    Parameters
    ----------
    reference_date : date
        Date to calculate offsets relative to.
    date_stencil : list of datetime.date, int and/or pairs of date/int; optional
        List of elements defining dates or date intervals.
        Each element can be:
            - a date object corresponding to an absolute date,
            - an int corresponding to an offset (in days) relative to reference_date,
            - a length-2 list [start, end] of dates or offsets, corresponding to a
              date interval (inclusive of both limits).
        Default [0]
    
    Returns
    -------
    list of tuple (pendulum.Date, pendulum.Date)
        List of pairs of date objects, each representing a date interval.
    """
    if date_stencil is None:
        date_stencil = [0]
    return stencil_to_date_pairs(stencil=date_stencil, reference_date=reference_date)


@task
def get_available_dates(
    api_url: str, cdr_types: Optional[Sequence[str]] = None
) -> List[pendulum.Date]:
    """
    Task to return a union of the dates for which data is available in FlowDB for the specified set of CDR types.
    
    Parameters
    ----------
    api_url : str
        The url to connect to FlowAPI
    cdr_types : list of str, optional
        Subset of CDR types for which to find available dates.
        If not provided, the union of available dates for all CDR types will be returned.
    
    Returns
    -------
    list of pendulum.Date
        List of available dates, in chronological order
    """
    context.logger.info(f"Getting available dates from FlowAPI at '{api_url}'.")
    conn = flowclient.connect(url=api_url, token=environ["FLOWCLIENT_TOKEN"])
    dates = flowclient.get_available_dates(connection=conn)
    if cdr_types is None:
        context.logger.debug(
            "No CDR types provided. Will return available dates for all CDR types."
        )
        cdr_types = dates.keys()
    else:
        context.logger.debug(f"Returning available dates for CDR types {cdr_types}.")
    dates_union = set().union(
        *[
            set(pendulum.parse(date, exact=True) for date in dates[cdr_type])
            for cdr_type in cdr_types
        ]
    )
    return sorted(list(dates_union))


@task
def filter_dates_by_earliest_date(
    dates: Sequence["datetime.date"], earliest_date: Optional["datetime.date"] = None
) -> List["datetime.date"]:
    """
    Filter task to return only dates later than or equal to earliest_date.
    If earliest_date is not provided, no filtering will be applied.
    
    Parameters
    ----------
    dates : list of date
        List of dates to filter
    earliest_date : date, optional
        Earliest date that will pass the filter
    
    Returns
    -------
    list of date
        Filtered list of dates
    """
    context.logger.info(f"Filtering out dates earlier than {earliest_date}.")
    if earliest_date is None:
        context.logger.debug(
            "No earliest date provided. Returning unfiltered list of dates."
        )
        return dates
    else:
        return [date for date in dates if date >= earliest_date]


@task
def filter_dates_by_stencil(
    dates: Sequence["datetime.date"],
    available_dates: Sequence["datetime.date"],
    date_stencil: Optional[stencil_type_alias] = None,
) -> List["datetime.date"]:
    """
    Filter task to return only dates for which all dates in the stencil are available.
    If no stencil is provided, no filtering will be applied
    (this is equivalent to 'stencil=[0]' if dates is a subset of available_dates).
    
    Parameters
    ----------
    dates : list of date
        List of dates to filter
    available_dates : list of date
        List of all dates that are available
    date_stencil : list of datetime.date, int and/or pairs of date/int; optional
        List of elements defining dates or date intervals.
        Each element can be:
            - a date object corresponding to an absolute date,
            - an int corresponding to an offset (in days) relative to a date in 'dates',
            - a length-2 list [start, end] of dates or offsets, corresponding to a
              date interval (inclusive of both limits).
    
    Returns
    -------
    list of date
        Filtered list of dates
    """
    context.logger.info("Filtering dates by stencil.")
    if date_stencil is None:
        context.logger.debug("No stencil provided. Returning unfiltered list of dates.")
        return dates
    else:
        context.logger.debug(
            f"Returning reference dates for which all dates in stencil {date_stencil} are available."
        )
        return [
            date
            for date in dates
            if dates_are_available(date_stencil, date, available_dates)
        ]


@task
def filter_dates_by_previous_runs(
    dates: Sequence["datetime.date"]
) -> List["datetime.date"]:
    """
    Filter task to return only dates for which the workflow hasn't previously run successfully.

    Parameters
    ----------
    dates : list of date
        List of dates to filter
    
    Returns
    -------
    list of date
        Filtered list of dates
    """
    context.logger.info(
        "Filtering out dates for which this workflow has already run successfully."
    )
    session = get_session()
    filtered_dates = [
        date
        for date in dates
        if workflow_runs.can_process(
            workflow_name=context.flow_name,
            workflow_params=context.parameters,
            reference_date=date,
            session=session,
        )
    ]
    session.close()
    return filtered_dates


@task
def record_workflow_in_process(reference_date: "datetime.date") -> None:
    """
    Add a row to the database to record that a workflow is running.

    Parameters
    ----------
    reference_date : date
        Reference date for which the workflow is running
    """
    context.logger.debug(
        f"Recording workflow run 'in_process' for reference date {reference_date}."
    )
    session = get_session()
    workflow_runs.set_state(
        workflow_name=context.flow_name,
        workflow_params=context.parameters,
        reference_date=reference_date,
        scheduled_start_time=context.scheduled_start_time,
        state="in_process",
        session=session,
    )
    session.close()


@task(trigger=all_successful)
def record_workflow_done(reference_date: "datetime.date") -> None:
    """
    Add a row to the database to record that a workflow completed successfully.

    Parameters
    ----------
    reference_date : date
        Reference date for which the workflow is running
    """
    context.logger.debug(
        f"Recording workflow run 'done' for reference date {reference_date}."
    )
    session = get_session()
    workflow_runs.set_state(
        workflow_name=context.flow_name,
        workflow_params=context.parameters,
        reference_date=reference_date,
        scheduled_start_time=context.scheduled_start_time,
        state="done",
        session=session,
    )
    session.close()


@task(trigger=any_failed)
def record_workflows_failed(reference_dates: List["datetime.date"]) -> None:
    """
    For each of the provided reference dates, if the corresponding workflow run is
    recorded as 'in_process', add a row to the database to record that the workflow failed.

    Parameters
    ----------
    reference_dates : list of date
        List of reference dates for which the workflow ran
    """
    # Note: unlike the other two 'record_workflows_*' tasks, this task is not mapped
    # (i.e. it takes a list of dates, not a single date). This is because if this
    # task was mapped, and a mistake when defining the workflow meant that a previous
    # task failed to map, this task would also fail to map and would therefore not run.
    context.logger.debug(
        f"Some tasks failed. Ensuring no workflow runs are left in 'in_process' state."
    )
    session = get_session()
    for reference_date in reference_dates:
        if not workflow_runs.is_done(
            workflow_name=context.flow_name,
            workflow_params=context.parameters,
            reference_date=reference_date,
            session=session,
        ):
            context.logger.debug(
                f"Recording workflow run 'failed' for reference date {reference_date}."
            )
            workflow_runs.set_state(
                workflow_name=context.flow_name,
                workflow_params=context.parameters,
                reference_date=reference_date,
                scheduled_start_time=context.scheduled_start_time,
                state="failed",
                session=session,
            )
    session.close()


@task
def mappable_dict(**kwargs) -> Dict[str, Any]:
    """
    Task that returns keyword arguments as a dict.
    Equivalent to calling dict(**kwargs) within a Flow context,
    except that this is a prefect task so it can be mapped.
    """
    return kwargs


@task
def papermill_execute_notebook(
    input_filename: str,
    output_label: str,
    parameters: Optional[Dict[str, Any]] = None,
    **kwargs,
) -> str:
    """
    Task to execute a notebook using Papermill.
    
    Parameters
    ----------
    input_filename : str
        Filename of input notebook (assumed to be in directory '${AUTOMATION_INPUTS_DIR}/notebooks/')
    output_label : str
        Label to append to output filename
    parameters : dict, optional
        Parameters to pass to the notebook
    **kwargs
        Additional keyword arguments to pass to papermill.execute_notebook
    
    Returns
    -------
    str
        Path to executed notebook
    """
    # Papermill injects all parameters into the notebook metadata, which then gets
    # json-serialised, so all parameters must be json serialisable
    # (see https://github.com/nteract/papermill/issues/412).
    # 'make_json_serialisable()' has the convenient side-effect of converting tuples to
    # lists, where we would otherwise have to register a custom papermill translator
    # for tuples.
    safe_params = make_json_serialisable(parameters)
    context.logger.info(
        f"Executing notebook '{input_filename}' with parameters {safe_params}."
    )

    # TODO: Shouldn't be reading env vars here
    inputs_dir = environ["AUTOMATION_INPUTS_DIR"]
    outputs_dir = environ["AUTOMATION_OUTPUTS_DIR"]
    output_filename = get_output_filename(input_filename, output_label)
    input_path = str(Path(inputs_dir) / "notebooks" / input_filename)
    output_path = str(Path(outputs_dir) / "notebooks" / output_filename)

    context.logger.debug(f"Output notebook will be '{output_path}'.")

    pm.execute_notebook(input_path, output_path, parameters=safe_params, **kwargs)

    context.logger.info(f"Finished executing notebook.")

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
        (assumed to be in directory '${AUTOMATION_INPUTS_DIR}/')
    
    Returns
    -------
    str
        Path to output PDF file
    """
    context.logger.info(f"Converting notebook '{notebook_path}' to PDF.")
    # TODO: Shouldn't be reading env vars here
    outputs_dir = environ["AUTOMATION_OUTPUTS_DIR"]
    if output_filename is None:
        output_filename = f"{Path(notebook_path).stem}.pdf"
    output_path = str(Path(outputs_dir) / "reports" / output_filename)

    with open(notebook_path) as nb_file:
        nb_read = nbformat.read(nb_file, as_version=4)

    if asciidoc_template is None:
        asciidoc_template = environ["AUTOMATION_ASCIIDOC_TEMPLATE"]
    # TODO: default asciidoc template probably won't be in the inputs dir.
    asciidoc_template_path = str(
        Path(environ["AUTOMATION_INPUTS_DIR"]) / asciidoc_template
    )
    context.logger.debug(
        f"Using template '{asciidoc_template_path}' to convert notebook to asciidoc."
    )

    exporter = ASCIIDocExporter(template_file=asciidoc_template_path)
    body, resources = exporter.from_notebook_node(nb_read)

    context.logger.debug("Converted notebook to asciidoc.")
    context.logger.debug("Converting asciidoc to PDF...")

    with TemporaryDirectory() as tmpdir:
        with open(f"{tmpdir}/tmp.asciidoc", "w") as f_tmp:
            f_tmp.write(body)
        for fname, content in resources["outputs"].items():
            with open(f"{tmpdir}/{fname}", "wb") as fout:
                fout.write(content)
        asciidoctor_pdf(f"{tmpdir}/tmp.asciidoc", o=output_path)

    context.logger.info(f"Created report '{output_filename}'.")

    return output_path
