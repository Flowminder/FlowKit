# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Prefect tasks used in workflows.
"""

import json
import papermill
import pendulum
import prefect
from pathlib import Path
from prefect import task
from typing import Any, Dict, List, Optional, Tuple

from .utils import (
    asciidoc_to_pdf,
    get_output_filename,
    get_params_hash,
    make_json_serialisable,
    notebook_to_asciidoc,
)


@task
def get_tag(reference_date: Optional["datetime.date"] = None) -> str:
    """
    Task to get a string to append to output filenames from this workflow run.
    The tag is unique for each set of workflow parameters and reference date.

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
    return f"{prefect.context.flow_name}_{params_hash}{ref_date_string}"


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
    # to other tasks in a workflow.
    return prefect.config.flowapi_url


@task
def mappable_dict(**kwargs) -> Dict[str, Any]:
    """
    Task that returns keyword arguments as a dict.
    Equivalent to passing dict(**kwargs) within a Flow context,
    except that this is a prefect task so it can be mapped.
    """
    return kwargs


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
