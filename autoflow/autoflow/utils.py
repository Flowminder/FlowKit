# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Utility functions.
"""

import datetime
import json
import nbconvert
import nbformat
import networkx as nx
import pendulum
from contextlib import contextmanager
from get_secret_or_env_var import getenv
from hashlib import md5
from pathlib import Path
from sh import asciidoctor_pdf
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from tempfile import TemporaryDirectory
from typing import Any, Dict, Iterator, List, Optional, Sequence, Set, Tuple, Union


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


date_or_offset = Union[int, datetime.date]  # Type alias for elements of date stencils
stencil_type_alias = List[
    Union[date_or_offset, List[date_or_offset]]
]  # Type alias for date stencils


class InvalidDatePairError(ValueError):
    """
    Custom error to raise if populating a date stencil results in a date pair with start_date > end_date.
    """

    pass


def offset_to_date(
    offset: date_or_offset, reference_date: datetime.date
) -> pendulum.Date:
    """
    Return a date corresponding to the offset from a reference date.

    Parameters
    ----------
    offset : int or datetime.date
        Either an integer number of days offset from reference date, or a date object.
        If a date object, this date will be returned.
    reference_date : datetime.date
        Date to calculate the offset relative to.

    Returns
    -------
    pendulum.Date
        reference_date + offset (if offset is an integer), or offset (if offset is a date).
    
    Raises
    ------
    TypeError
        If type(offset) is not either int or datetime.date
    """
    if isinstance(offset, datetime.date):
        date_from_offset = pendulum.date(offset.year, offset.month, offset.day)
    elif isinstance(offset, int):
        date_from_offset = pendulum.date(
            reference_date.year, reference_date.month, reference_date.day
        ).add(days=offset)
    else:
        raise TypeError(
            f"Invalid type for offset: expected 'date' or 'int', not '{type(offset).__name__}'"
        )
    return date_from_offset


# TODO: There are several helper functions for date stencils here.
# Might be better to make them methods of a DateStencil class.


def stencil_to_date_pairs(
    stencil: stencil_type_alias, reference_date: datetime.date
) -> List[Tuple[pendulum.Date, pendulum.Date]]:
    """
    Given a stencil of dates, date offsets and/or date intervals, and a reference
    date to calculate the offsets relative to, return a list of date pairs representing
    date intervals (inclusive of both limits).

    Parameters
    ----------
    stencil : list of datetime.date, int and/or pairs of date/int
        List of elements defining dates or date intervals.
        Each element can be:
            - a date object corresponding to an absolute date,
            - an int corresponding to an offset (in days) relative to reference_date,
            - a length-2 list [start, end] of dates or offsets, corresponding to a
              date interval (inclusive of both limits).
    reference_date : datetime.date
        Date to calculate offsets relative to.
    
    Returns
    -------
    list of tuple (pendulum.Date, pendulum.Date)
        List of pairs of date objects, each representing a date interval.
    
    Raises
    ------
    TypeError
        If elements of stencil have the wrong type
    ValueError
        If list elements of stencil do not have length 2
    InvalidDatePairError
        If the stencil results in a date pair with start_date > end_date
    """
    date_pairs = []
    for element in stencil:
        if isinstance(element, list):
            if len(element) != 2:
                raise ValueError(
                    "Expected date interval to be a list of length 2 (in format [start, end]), "
                    "but got list of length {len(element)}."
                )
            start_date = offset_to_date(element[0], reference_date)
            end_date = offset_to_date(element[1], reference_date)
            if start_date > end_date:
                raise InvalidDatePairError(
                    f"Stencil contains invalid date pair ({start_date}, {end_date}) for reference date {reference_date}."
                )
            date_pairs.append((start_date, end_date))
        else:
            date = offset_to_date(element, reference_date)
            date_pairs.append((date, date))
    return date_pairs


def stencil_to_set_of_dates(
    stencil: stencil_type_alias, reference_date: datetime.date
) -> Set[pendulum.Date]:
    """
    Given a stencil of dates, date offsets and/or date intervals, and a reference
    date to calculate the offsets relative to, return the corresponding set of dates.

    Parameters
    ----------
    stencil : list of datetime.date, int and/or pairs of date/int
        List of elements defining dates or date intervals.
        Each element can be:
            - a date object corresponding to an absolute date,
            - an int corresponding to an offset (in days) relative to reference_date,
            - a length-2 list [start, end] of dates or offsets, corresponding to a
              date interval (inclusive of both limits).
    reference_date : datetime.date
        Date to calculate offsets relative to.

    Returns
    -------
    set of pendulum.Date
        Set of dates represented by the stencil
    """
    date_pairs = stencil_to_date_pairs(stencil, reference_date)
    dates = set().union(*[pendulum.period(pair[0], pair[1]) for pair in date_pairs])
    return dates


def dates_are_available(
    stencil: stencil_type_alias,
    reference_date: datetime.date,
    available_dates: Sequence[datetime.date],
) -> bool:
    """
    Check whether all dates represented by a stencil for a particular date are
    included in a list of available dates.

    Parameters
    ----------
    stencil : list of datetime.date, int and/or pairs of date/int
        List of elements defining dates or date intervals.
        Each element can be:
            - a date object corresponding to an absolute date,
            - an int corresponding to an offset (in days) relative to reference_date,
            - a length-2 list [start, end] of dates or offsets, corresponding to a
              date interval (inclusive of both limits).
    reference_date : datetime.date
        Date to calculate offsets relative to.
    available_dates : list of datetime.date
        List of available dates
    
    Returns
    -------
    bool
        True if all dates are available, False otherwise.
    
    Notes
    -----

    If the stencil is not valid for the given reference date (i.e. contains invalid
    date pairs), this function will return False.
    """
    try:
        dates_from_stencil = stencil_to_set_of_dates(
            stencil, reference_date=reference_date
        )
    except InvalidDatePairError:
        return False
    return dates_from_stencil.issubset(set(available_dates))


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
        Names of parameters used within workflow, which cannot be used as notebook labels.
    
    Returns
    -------
    set of str
        Names of parameters used by notebooks which are not either reserved parameter names or notebook labels
    """
    if reserved_parameter_names is None:
        reserved_parameter_names = set()
    # Check that notebook task labels do not include reserved parameter names
    # TODO: This should be checked elsewhere
    forbidden_labels = reserved_parameter_names.intersection(notebooks.keys())
    if forbidden_labels:
        raise ValueError(
            f"Notebook labels {forbidden_labels} are forbidden for this workflow. "
            f"Reserved parameter names are {reserved_parameter_names}."
        )
    # Parameters requested in notebooks
    notebook_parameter_names = set.union(
        *[set(notebook["parameters"].values()) for notebook in notebooks.values()]
    )
    # Additional parameters required for notebooks
    additional_parameter_names_for_notebooks = notebook_parameter_names.difference(
        reserved_parameter_names
    ).difference(notebooks.keys())
    return additional_parameter_names_for_notebooks


def sort_notebook_labels(notebooks: Dict[str, Dict[str, Any]]) -> List[str]:
    """
    Perform a topological sort on a dictionary of notebook task specifications.

    Parameters
    ----------
    notebooks : dict
        Dictionary of dictionaries describing notebook tasks.
    
    Returns
    -------
    list of str
        List of notebook labels, sorted so that no notebook depends on another
        notebook that comes after it in the list.
    """
    notebooks_graph = nx.DiGraph(
        {
            key: [
                value
                for value in notebooks[key]["parameters"].values()
                if value in notebooks
            ]
            for key in notebooks
        }
    ).reverse()
    try:
        sorted_notebook_labels = list(nx.topological_sort(notebooks_graph))
    except nx.NetworkXUnfeasible:
        raise ValueError("Notebook specifications contain cyclic dependencies.")

    return sorted_notebook_labels


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
