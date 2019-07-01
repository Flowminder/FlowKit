# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Various simple utilities.
"""

import datetime
import logging
import re
from contextlib import contextmanager
import networkx as nx
import structlog
import sys
from io import BytesIO
from pathlib import Path
from pglast import prettify
from psycopg2._psycopg import adapt
from time import sleep
from typing import List, Union, Tuple

logger = structlog.get_logger("flowmachine.debug", submodule=__name__)


def getsecret(key: str, default: str) -> str:
    """
    Get a value from docker secrets (i.e. read it from a file in
    /run/secrets), return a default if the file is not there.

    Parameters
    ----------
    key: str
        Name of the secret.
    default: str
        Default value to return if the file does not exist

    Returns
    -------
    str
        Value in the file, or default
    """
    try:
        with open(Path("/run/secrets") / key, "r") as fin:
            return fin.read().strip()
    except FileNotFoundError:
        return default


def parse_datestring(
    datestring: Union[str, datetime.datetime, datetime.date]
) -> datetime.datetime:
    """

    Parameters
    ----------
    datestring : str, datetime, or date
        ISO string date, or a datetime or date object

    Returns
    -------
    datetime.datetime

    """
    if isinstance(datestring, datetime.datetime):
        return datestring
    elif isinstance(datestring, datetime.date):
        return datetime.datetime(datestring.year, datestring.month, datestring.day)
    else:
        try:
            return datetime.datetime.strptime(datestring, "%Y-%m-%d %X")
        except ValueError:
            try:
                return datetime.datetime.strptime(datestring, "%Y-%m-%d %H:%M")
            except ValueError:
                try:
                    return datetime.datetime.strptime(datestring, "%Y-%m-%d")
                except ValueError:
                    raise ValueError(
                        "{} could not be parsed as as date.".format(datestring)
                    )


def list_of_dates(start, stop):
    """

    Parameters
    ----------
    start, stop : str
        yyyy-mm-dd format datestrings.

    Returns
    -------
    list of yyyy-mm-dd format datestrings between start and stop (inclusive)

    """

    d1 = parse_datestring(start)
    d2 = parse_datestring(stop)

    if d2 < d1:
        raise ValueError("The start date is later than the stop date.")

    total_days = (d2 - d1).days

    # Note the plus one, as we want to include the final day
    all_dates = [
        (d1 + datetime.timedelta(i)).strftime("%Y-%m-%d") for i in range(total_days + 1)
    ]

    return all_dates


def time_period_add(date, n, unit="days"):
    """
    Adds n days to the date (represented as a string). Or alternatively add hours or
    minutes to the date.

    Parameters
    ----------
    date : str
        Date to add to in yyyy-mm-dd format
    n : int
        Number of units to add
    unit : str, default 'days'
        Type of unit to add to the date

    Returns
    -------
    str
        Altered date string

    Examples
    --------

    >>> time_period_add('2016-01-01', 3)
    '2016-01-04'
    """
    new_date = parse_datestring(date) + datetime.timedelta(**{unit: n})
    # Now for convenience if the date has no time component then we want to
    # simply return the date string, i.e. without the time component. Otherwise
    # we need to return the whole thing.
    date_string = new_date.strftime("%Y-%m-%d %X")
    if date_string.endswith("00:00:00"):
        return date_string.split(" ")[0]
    else:
        return date_string


def get_dist_query_string(*, lon1, lat1, lon2, lat2):
    """
    function for getting the distance
    query string between two lon-lat points.
    """
    return f"""
    ST_Distance(ST_Point({lon1}, {lat1})::geography,
                ST_point({lon2}, {lat2})::geography)
    """


def proj4string(conn, crs=None):
    """
    Provide a proj4 string for the input, or by default
    return the wsg84 proj4 string.

    Parameters
    ----------
    conn : Connection
        FlowMachine db connection to use to get the proj4 string
    crs : int or str
        An integer EPSG code, or a proj4 string

    Returns
    -------
    str
        Proj4 string for the input crs.

    """
    if isinstance(crs, int):
        try:
            proj4_string = conn.fetch(
                "SELECT proj4text FROM spatial_ref_sys WHERE srid={}".format(crs)
            )[0][0]
        except IndexError:
            raise ValueError("{} is not a valid EPSG code.".format(crs))
    elif isinstance(crs, str):
        proj4_string = crs
    elif crs is None:
        proj4_string = "+proj=longlat +datum=WGS84 +no_defs"
    else:
        raise ValueError("{} cannot be converted to proj4.".format(crs))
    return proj4_string.strip()


def pretty_sql(
    sql,
    compact_lists_margin=0,
    split_string_literals_threshold=0,
    special_functions=True,
    comma_at_eoln=True,
):
    """
    Prettify and validate the syntax of an SQL query, using pglast

    Parameters
    ----------
    sql : str
        SQL to prettyify and validate
    compact_lists_margin : int, default 0
        Use compact form for lists shorter than this
    split_string_literals_threshold : int, default 0
        Split strings (in the sql) longer than this threshold
    special_functions : bool, default True
        Translate some special functions to their more commonly used forms
    comma_at_eoln

    Raises
    ------

    pglast.parser.ParseError
        Raises a parse error if the query syntax was bad.

    See Also
    --------
    pglast.prettify: Function wrapped, use this for additional prettification options

    Returns
    -------
    str
        The prettified string.

    """

    return prettify(
        sql,
        compact_lists_margin=compact_lists_margin,
        split_string_literals_threshold=split_string_literals_threshold,
        special_functions=special_functions,
        comma_at_eoln=comma_at_eoln,
    )


def _makesafe(x):
    """
    Function that converts input into a PostgreSQL readable.
    """
    return adapt(x).getquoted().decode()


def get_name_and_alias(column_name: str) -> Tuple[str]:
    """
    Given a column name string, return the column name and alias (if there is
    one), or return the provided column name twice if there is no alias.

    Examples
    --------
    >>> get_name_and_alias("col AS alias")
      ('col', 'alias')
    >>> get_name_and_alias("col")
      ('col', 'col')
    >>> get_name_and_alias("table.col")
      ('table.col', 'col')
    >>> get_name_and_alias("table.col as alias")
      ('table.col', 'alias')
    """
    column_name_split = re.split(" as ", column_name, flags=re.IGNORECASE)
    if len(column_name_split) == 1:
        return column_name_split[0].strip(), column_name_split[0].strip().split(".")[-1]
    else:
        return column_name_split[0].strip(), column_name_split[-1].strip()


def _sleep(seconds_to_sleep):
    # Private function to facilitate testing
    # monkeypatch this to avoid needing to monkeypatch time.sleep
    sleep(seconds_to_sleep)


def convert_dict_keys_to_strings(input_dict):
    """
    Return a copy of the input dictionary where all keys are converted to strings.
    This is necessary if the dictionary needs to be valid JSON.

    Parameters
    ----------
    input_dict : dict
        The input dictionary.

    Returns
    -------
    dict
        A copy of `input_dict` with all keys converted to strings.
    """
    result = {}
    for key, value in input_dict.items():
        new_key = str(key)
        if isinstance(value, dict):
            result[new_key] = convert_dict_keys_to_strings(value)
        else:
            result[new_key] = value
    return result


def to_nested_list(d):
    """
    Helper function to recursively convert an input dictionary to a nested list.

    Parameters
    ----------
    d : dict
        Input dictionary (can be nested).

    Returns
    -------
    list of tuples
    """
    if isinstance(d, dict):
        return [(key, to_nested_list(value)) for key, value in d.items()]
    elif isinstance(d, (list, tuple)):
        return [to_nested_list(x) for x in d]
    else:
        return d


def sort_recursively(d):
    """
    Helper function to sort a dictionary recursively (including any dicts or sequences inside it).

    Parameters
    ----------
    d : dict
        Input dictionary (can be nested).

    Returns
    -------
    dict
        The sorted dictionary.
    """
    if isinstance(d, dict):
        d_new = dict()
        for key in sorted(d.keys()):
            d_new[key] = sort_recursively(d[key])
        return d_new
    elif isinstance(d, list):
        return sorted(d, key=to_nested_list)
    else:
        return d


def print_dependency_tree(query_obj, show_stored=False, stream=None, indent_level=0):
    """
    Print the dependencies of a flowmachine query in a tree-like structure.

    Parameters
    ----------
    query_obj : Query
        An instance of a query object.
    show_stored : bool, optional
        If True, show for each query whether it is stored or not. Default: False.
    stream : io.IOBase, optional
        The stream to which the output should be written (default: stdout).
    indent_level : int
        The current level of indentation.
    """
    stream = stream or sys.stdout
    indent_level = indent_level or 0

    indent_per_level = 3
    indent = " " * (indent_per_level * indent_level - 1)
    prefix = "" if indent_level == 0 else "- "
    fmt = "query_id" if not show_stored else "query_id,is_stored"
    stream.write(f"{indent}{prefix}{query_obj:{fmt}}\n")
    deps_sorted_by_query_id = sorted(query_obj.dependencies, key=lambda q: q.md5)
    for dep in deps_sorted_by_query_id:
        print_dependency_tree(
            dep, indent_level=indent_level + 1, stream=stream, show_stored=show_stored
        )


def _get_query_attrs_for_dependency_graph(query_obj, analyse=False):
    """
    Helper method which returns information about this query for use in a dependency graph.

    Parameters
    ----------
    query_obj : Query
        Query object to return information for.
    analyse : bool
        Set to True to get actual runtimes for queries. Note that this will actually run the query!

    Returns
    -------
    dict
        Dictionary containing the keys "name", "stored", "cost" and "runtime" (the latter is only
        present if `analyse=True`.
        Example return value: `{"name": "DailyLocation", "stored": False, "cost": 334.53, "runtime": 161.6}`
    """
    expl = query_obj.explain(format="json", analyse=analyse)[0]
    attrs = {}
    attrs["name"] = query_obj.__class__.__name__
    attrs["stored"] = query_obj.is_stored
    attrs["cost"] = expl["Plan"]["Total Cost"]
    if analyse:
        attrs["runtime"] = expl["Execution Time"]
    return attrs


def calculate_dependency_graph(query_obj, analyse=False):
    """
    Produce a graph of all the queries that go into producing this one, with their estimated
    run costs, and whether they are stored as node attributes.

    The resulting networkx object can then be visualised, or analysed. When visualised,
    nodes corresponding to stored queries will be rendered green. See the function
    `plot_dependency_graph()` for a convenient way of plotting a dependency graph directly
    for visualisation in a Jupyter notebook.

    The dependency graph includes the estimated cost of the query in the 'cost' attribute,
    the query object the node represents in the 'query_object' attribute, and with the analyse
    parameter set to true, the actual running time of the query in the `runtime` attribute.

    Parameters
    ----------
    query_obj : Query
        Query object to produce a dependency graph for.
    analyse : bool
        Set to True to get actual runtimes for queries. Note that this will actually run the query!

    Returns
    -------
    networkx.DiGraph

    Examples
    --------

    If you don't want to visualise the dependency graph directly (for example
    using `plot_dependency_graph()`, you can export it to a .dot file as follows:

    >>> import flowmachine
    >>> from flowmachine.features import daily_location
    >>> from networkx.drawing.nx_agraph import write_dot
    >>> flowmachine.connect()
    >>> G = daily_location("2016-01-01").dependency_graph()
    >>> write_dot(G, "daily_location_dependencies.dot")
    >>> G = daily_location("2016-01-01").dependency_graph(True)
    >>> write_dot(G, "daily_location_dependencies_runtimes.dot")

    The resulting .dot file then be converted to a .pdf file using the external
    tool `dot` which comes as part of the [GraphViz](https://www.graphviz.org/) package:<br />

    ```
    $ dot -Tpdf daily_location_dependencies.dot -o daily_location_dependencies.pdf
    ```

    [Graphviz]: https://www.graphviz.org/

    Notes
    -----
    The queries listed as dependencies are not _guaranteed_ to be
    used in the actual running of a query, only to be referenced by it.
    """
    g = nx.DiGraph()
    openlist = [(0, query_obj)]
    deps = []

    while openlist:
        y, x = openlist.pop()
        deps.append((y, x))

        openlist += list(zip([x] * len(x.dependencies), x.dependencies))

    _, y = zip(*deps)
    for n in set(y):
        attrs = _get_query_attrs_for_dependency_graph(n, analyse=analyse)
        attrs["shape"] = "rect"
        attrs["query_object"] = n
        attrs["label"] = f"{attrs['name']}. Cost: {attrs['cost']}"
        if analyse:
            attrs["label"] += " Actual runtime: {}.".format(attrs["runtime"])
        if attrs["stored"]:
            attrs["fillcolor"] = "#b3de69"  # light green
            attrs["style"] = "filled"
        g.add_node(f"x{n.md5}", **attrs)

    for x, y in deps:
        if x != 0:
            g.add_edge(*[f"x{z.md5}" for z in (x, y)])

    return g


def plot_dependency_graph(
    query_obj, analyse=False, format="png", width=None, height=None
):
    """
    Plot a graph of all the queries that go into producing this one (see `calculate_dependency_graph`
    for more details). This returns an IPython.display object which can be directly displayed in
    Jupyter notebooks.

    Note that this requires the IPython and pygraphviz packages to be installed.

    Parameters
    ----------
    query_obj : Query
        Query object to plot a dependency graph for.
    analyse : bool
        Set to True to get actual runtimes for queries. Note that this will actually run the query!
    format : {"png", "svg"}
        Output format of the resulting
    width : int
        Width in pixels to which to constrain the image. Note this is only supported for format="png".
    height : int
        Height in pixels to which to constrain the image. Note this is only supported for format="png".

    Returns
    -------
    IPython.display.Image or IPython.display.SVG
    """
    try:  # pragma: no cover
        from IPython.display import Image, SVG
    except ImportError:
        raise ImportError("requires IPython ", "https://ipython.org/")

    try:  # pragma: no cover
        import pygraphviz
    except ImportError:
        raise ImportError(
            "requires the Python package `pygraphviz` (as well as the system package `graphviz`) - make sure both are installed ",
            "http://pygraphviz.github.io/",
        )

    if format not in ["png", "svg"]:
        raise ValueError(f"Unsupported output format: '{format}'")

    G = calculate_dependency_graph(query_obj, analyse=analyse)
    A = nx.nx_agraph.to_agraph(G)
    s = BytesIO()
    A.draw(s, format=format, prog="dot")

    if format == "png":
        result = Image(s.getvalue(), width=width, height=height)
    elif format == "svg":
        if width is not None or height is not None:  # pragma: no cover
            logger.warning(
                "The arguments 'width' and 'height' are not supported with format='svg'."
            )
        result = SVG(s.getvalue().decode("utf8"))
    else:
        raise ValueError(f"Unsupported output format: '{format}'")

    return result
