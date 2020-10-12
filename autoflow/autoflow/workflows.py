# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Contains tasks and functions for constructing Prefect workflows.
"""

from pathlib import Path
from typing import Any, Dict, Optional, OrderedDict

import papermill
import prefect

from autoflow.utils import (
    asciidoc_to_pdf,
    get_additional_parameter_names_for_notebooks,
    get_output_filename,
    get_params_hash,
    make_json_serialisable,
    notebook_to_asciidoc,
)


# Tasks -----------------------------------------------------------------------


@prefect.task
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


@prefect.task
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


@prefect.task
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


@prefect.task
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
) -> prefect.Flow:
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
    prefect.Flow
        Workflow that runs the notebooks.
    """
    # Get parameter names from notebooks dict
    parameter_names = get_additional_parameter_names_for_notebooks(
        notebooks=notebooks, reserved_parameter_names={"flowapi_url"}
    )

    # Define workflow
    with prefect.Flow(name=name) as workflow:
        # Parameters
        parameter_tasks = {
            pname: prefect.Parameter(pname)
            for pname in parameter_names.union({"reference_date", "date_ranges"})
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
                    k: parameter_tasks[v]
                    for k, v in notebook.get("parameters", {}).items()
                },
            )
            if "output" in notebook:
                # Create PDF report from notebook
                convert_notebook_to_pdf(
                    notebook_path=parameter_tasks[key],
                    asciidoc_template=notebook["output"]["template"],
                )

    return workflow
