from prefect import task
import papermill as pm
from typing import Optional, Dict, Any
import nbformat
from nbconvert import ASCIIDocExporter
from tempfile import TemporaryDirectory
from sh import asciidoctor_pdf
from pathlib import Path
from datetime import datetime

from .utils import get_output_filename


@task
def get_timestamp() -> str:
    """
    Task to get a timestamp string representing the current datetime
    """
    return datetime.now().isoformat(timespec='seconds')

@task
def papermill_execute_notebook(
    input_path: str, output_dir: str, output_label: str, parameters: Optional[Dict[str, Any]] = None, **kwargs
) -> str:
    """
    Task to execute a notebook using Papermill
    
    Parameters
    ----------
    input_path : str
        Full path to input notebook
    output_dir : str
        Directory in which to save executed notebook
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
    output_path = get_output_filename(
        input_path, output_dir, output_label
    )
    pm.execute_notebook(
        input_path,
        output_path,
        parameters=parameters,
        **kwargs
    )
    return output_path


@task
def convert_notebook_to_pdf(
    notebook_path: str, output_dir: str, output_filename: Optional[str] = None, asciidoc_template_file: Optional[str] = None
) -> str:
    """
    Task to convert a notebook to PDF, via asciidoc (without executing the notebook).
    
    Parameters
    ----------
    notebook_path : str
        Path to input notebook
    output_dir : str
        Directory in which to save output PDF file
    output_filename : str, optional
        Filename for output PDF file.
        If not provided, this will be the name of the input notebook with the extension changed to '.pdf'
    asciidoc_template_file : str, optional
        Path to a template to use when exporting to asciidoc
    
    Returns
    -------
    str
        Path to output PDF file
    """
    if output_filename is None:
        output_filename = f"{Path(notebook_path).stem}.pdf"
    output_path = f"{output_dir}/{output_filename}"
    with open(notebook_path) as nb_file:
        nb_read = nbformat.read(nb_file, as_version=4)

    exporter = ASCIIDocExporter(template_file=asciidoc_template_file)
    body, resources = exporter.from_notebook_node(nb_read)

    with TemporaryDirectory() as tmpdir:
        with open(f"{tmpdir}/tmp.asciidoc", "w") as f_tmp:
            f_tmp.write(body)
        for fname, content in resources["outputs"].items():
            with open(f"{tmpdir}/{fname}", "wb") as fout:
                fout.write(content)
        asciidoctor_pdf(f"{tmpdir}/tmp.asciidoc", o=output_path)
    
    return output_path