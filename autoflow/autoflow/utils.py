# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Utility functions.
"""

import collections
import json
from contextlib import contextmanager
from hashlib import md5
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import Any, Dict, Iterator, Optional, OrderedDict, Set, Tuple, Union

import nbconvert
import nbformat
import networkx as nx
import pendulum
from get_secret_or_env_var import getenv
from sh import asciidoctor_pdf
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker


def get_output_filename(input_filename: str, tag: str = "") -> str:
    """
    Given an input filename, construct an output filename with the same extension,
    with a timestamp (and optionally a tag) added to the stem.

    Parameters
    ----------
    input_filename : str
        Input filename
    tag : str, optional
        A tag to append to the file stem

    Returns
    -------
    str
        Output filename
    """
    input_filename = Path(input_filename)
    now_string = pendulum.now("utc").format("YYYYMMDDTHHmmss[Z]")
    tag = f"__{tag}" if tag != "" else tag
    return f"{input_filename.stem}{tag}__{now_string}{input_filename.suffix}"


def get_params_hash(parameters: Dict[str, Any]) -> str:
    """
    Generate a md5 hash string from a dictionary of parameters.
    The dictionary is dumped to a json string before hashing.

    Parameters
    ----------
    parameters : dict
        Dictionary of parameters to use for creating the hash.

    Returns
    -------
    str
        md5 hash of the parameters dict
    """
    return md5(
        json.dumps(dict(parameters), sort_keys=True, default=str).encode()
    ).hexdigest()


def get_session(db_uri: str) -> "sqlalchemy.orm.session.Session":
    """
    Create a sqlalchemy session.

    Parameters
    ----------
    db_uri : str
        Database URI

    Returns
    -------
    Session
        A sqlalchemy session
    """
    # TODO: This seems like the wrong place to be reading a secret / env var,
    # but we can't put a docker secret in the prefect config.
    full_db_uri = db_uri.format(getenv("AUTOFLOW_DB_PASSWORD", ""))
    engine = create_engine(full_db_uri)
    return sessionmaker(bind=engine)()


@contextmanager
def session_scope(db_uri: str) -> Iterator["sqlalchemy.orm.session.Session"]:
    """
    Provide a scoped sqlalchemy session for interacting with the database.

    Parameters
    ----------
    db_uri : str
        Database URI

    Yields
    ------
    Session
        A sqlalchemy session
    """
    session = get_session(db_uri)
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


def make_json_serialisable(obj: Any) -> Union[dict, list, str, int, float, bool, None]:
    """
    Helper function to convert an object's type so that it can be serialised using
    the default json serialiser. Non-serialisable types are converted to strings.

    Parameters
    ----------
    obj : any
        Object to be converted.

    Returns
    -------
    dict, list, str, int, float, bool or None
        The same object after dumping to json (converting to str if necessary) and loading
    """
    return json.loads(json.dumps(obj, default=str))


def get_additional_parameter_names_for_notebooks(
    notebooks: Dict[str, Dict[str, Any]],
    reserved_parameter_names: Optional[Set[str]] = None,
) -> Set[str]:
    """
    Extract parameter names from a list of notebook task specifications.

    Parameters
    ----------
    notebooks : dict
        Dictionary of dictionaries describing notebook tasks.
    reserved_parameter_names : set of str, optional
        Names of parameters used within workflow.

    Returns
    -------
    set of str
        Names of parameters used by notebooks which must be passed to a workflow.
    """
    # Parameters requested in notebooks
    notebook_parameter_names = set.union(
        *[
            set(notebook.get("parameters", {}).values())
            for notebook in notebooks.values()
        ]
    )
    # Parameters available to notebooks, which are not passed externally as flow parameters
    internal_parameter_names = (reserved_parameter_names or set()).union(
        notebooks.keys()
    )
    # Additional parameters required for notebooks
    additional_parameter_names_for_notebooks = notebook_parameter_names.difference(
        internal_parameter_names
    )
    return additional_parameter_names_for_notebooks


def sort_notebooks(
    notebooks: Dict[str, Dict[str, Any]]
) -> OrderedDict[str, Dict[str, Any]]:
    """
    Perform a topological sort on a dictionary of notebook task specifications.

    Parameters
    ----------
    notebooks : dict
        Dictionary of dictionaries describing notebook tasks.

    Returns
    -------
    OrderedDict
        Ordered dict of notebook task dicts, ordered so that no notebook depends on another
        notebook that comes after it.

    Raises
    ------
    ValueError
        If the notebook specifications contain circular dependencies.
    """
    notebooks_graph = nx.DiGraph(
        {
            key: [
                value
                for value in notebooks[key].get("parameters", {}).values()
                if value in notebooks
            ]
            for key in notebooks
        }
    ).reverse()
    try:
        sorted_notebook_keys = list(nx.topological_sort(notebooks_graph))
    except nx.NetworkXUnfeasible:
        raise ValueError("Notebook specifications contain circular dependencies.")

    return collections.OrderedDict(
        (key, notebooks[key]) for key in sorted_notebook_keys
    )


def notebook_to_asciidoc(
    notebook_path: str, asciidoc_template_path: Optional[str] = None
) -> Tuple[str, "nbconvert.exporters.exporter.ResourcesDict"]:
    """
    Convert a Jupyter notebook to ASCIIDoc, using nbconvert.

    Parameters
    ----------
    notebook_path : str
        Path to notebook
    asciidoc_template_path : str, optional
        Path to a template file to use for the conversion.
        If not provided, the default nbconvert asciidoc template will be used.

    Returns
    -------
    str
        The resulting converted notebook.
    ResourcesDict
        Dictionary of resources used prior to and during the conversion process.
    """
    with open(notebook_path) as nb_file:
        nb_read = nbformat.read(nb_file, as_version=4)

    if asciidoc_template_path is None:
        exporter = nbconvert.ASCIIDocExporter()
    else:
        exporter = nbconvert.ASCIIDocExporter(template_file=asciidoc_template_path)
    body, resources = exporter.from_notebook_node(nb_read)

    return body, resources


def asciidoc_to_pdf(
    body: str, resources: "nbconvert.exporters.exporter.ResourcesDict", output_path: str
) -> None:
    """
    Convert an ASCIIDoc document to PDF, using asciidoctor-pdf.

    Parameters
    ----------
    body : str
        Content of the ASCIIDoc document
    resources : ResourcesDict
        Dictionary of resources, as returned from nbconvert exporter
    """
    with TemporaryDirectory() as tmpdir:
        with open(f"{tmpdir}/tmp.asciidoc", "w") as f_tmp:
            f_tmp.write(body)
        for fname, content in resources["outputs"].items():
            with open(f"{tmpdir}/{fname}", "wb") as fout:
                fout.write(content)
        asciidoctor_pdf(f"{tmpdir}/tmp.asciidoc", o=output_path)
