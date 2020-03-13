# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


import networkx as nx
import sys
import structlog
from io import BytesIO
from typing import Union, Tuple, Dict, Sequence, Callable, Any, Optional, List, Set
from concurrent.futures import wait
from functools import lru_cache

from flowmachine.core.context import get_redis, get_db
from flowmachine.core.errors import UnstorableQueryError
from flowmachine.core.query_state import QueryStateMachine, QueryState

logger = structlog.get_logger("flowmachine.debug", submodule=__name__)


def print_dependency_tree(
    query_obj: "Query",
    show_stored: bool = False,
    stream: Optional["IOBase"] = None,
    indent_level: int = 0,
) -> None:
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
    deps_sorted_by_query_id = sorted(query_obj.dependencies, key=lambda q: q.query_id)
    for dep in deps_sorted_by_query_id:
        print_dependency_tree(
            dep, indent_level=indent_level + 1, stream=stream, show_stored=show_stored
        )


def _assemble_dependency_graph(
    dependencies: Sequence[Tuple[Union["Query", None], "Query"]],
    attrs_func: Callable[["Query"], Dict[str, Any]],
) -> nx.DiGraph:
    """
    Helper function to assemble a dependency graph from a list of dependencies.

    Parameters
    ----------
    dependencies : list of tuples (query1, query2) or (None, query2)
        Dependencies between query objects.
    attrs_func : function
        Function that returns a dict of node attributes for a query object.
    
    Returns
    -------
    networkx.DiGraph
    """
    g = nx.DiGraph()

    if dependencies:
        _, y = zip(*dependencies)
        for n in set(y):
            attrs = attrs_func(n)
            g.add_node(f"x{n.query_id}", **attrs)

    for x, y in dependencies:
        if x is not None:
            g.add_edge(*[f"x{z.query_id}" for z in (x, y)])

    return g


def _get_query_attrs_for_dependency_graph(
    query_obj: "Query", analyse: bool = False
) -> Dict[str, Any]:
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


def calculate_dependency_graph(query_obj: "Query", analyse: bool = False) -> nx.DiGraph:
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
    deps = get_dependency_links(query_obj)

    def get_node_attrs(q):
        attrs = _get_query_attrs_for_dependency_graph(q, analyse=analyse)
        attrs["shape"] = "rect"
        attrs["query_object"] = q
        attrs["label"] = f"{attrs['name']}. Cost: {attrs['cost']}"
        if analyse:
            attrs["label"] += " Actual runtime: {}.".format(attrs["runtime"])
        if attrs["stored"]:
            attrs["fillcolor"] = "#b3de69"  # light green
            attrs["style"] = "filled"
        return attrs

    return _assemble_dependency_graph(dependencies=deps, attrs_func=get_node_attrs)


@lru_cache(maxsize=256)
def get_dependency_links(
    query_obj: "Query",
) -> List[Tuple[Union[None, "Query"], "Query"]]:
    openlist = [(None, query_obj)]
    deps = []
    while openlist:
        y, x = openlist.pop()
        deps.append((y, x))

        openlist += list(zip([x] * len(x.dependencies), x.dependencies))
    return deps


def unstored_dependencies_graph(query_obj: "Query") -> nx.DiGraph:
    """
    Produce a dependency graph of the unstored queries on which this query depends.

    Parameters
    ----------
    query_obj : Query
        Query object to produce a dependency graph for.
    
    Returns
    -------
    networkx.DiGraph

    Notes
    -----
    If store() or invalidate_db_cache() is called on any query while this
    function is executing, the resulting graph may not be correct.
    The queries listed as dependencies are not _guaranteed_ to be
    used in the actual running of a query, only to be referenced by it.
    """
    deps = []

    if not query_obj.is_stored:
        openlist = list(
            zip([query_obj] * len(query_obj.dependencies), query_obj.dependencies)
        )

        while openlist:
            y, x = openlist.pop()
            if y is query_obj:
                # We don't want to include this query in the graph, only its dependencies.
                y = None
            # Wait for query to complete before checking whether it's stored.
            q_state_machine = QueryStateMachine(
                get_redis(), x.query_id, get_db().conn_id
            )
            q_state_machine.wait_until_complete()
            if not x.is_stored:
                deps.append((y, x))
                openlist += list(zip([x] * len(x.dependencies), x.dependencies))

    def get_node_attrs(q):
        attrs = {}
        attrs["query_object"] = q
        attrs["name"] = q.__class__.__name__
        attrs["stored"] = False
        attrs["shape"] = "rect"
        attrs["label"] = f"{attrs['name']}."
        return attrs

    return _assemble_dependency_graph(dependencies=deps, attrs_func=get_node_attrs)


def plot_dependency_graph(
    query_obj: "Query",
    analyse: bool = False,
    format: str = "png",
    width: Optional[int] = None,
    height: Optional[int] = None,
) -> Union["IPython.display.Image", "IPython.display.SVG"]:
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


def store_queries_in_order(dependency_graph: nx.DiGraph) -> Dict[str, "Future"]:
    """
    Execute queries in an order that ensures each query store is triggered after its dependencies.

    Parameters
    ----------
    dependency_graph : networkx.DiGraph
        Dependency graph of query objects to be stored
    
    Returns
    -------
    dict
        Mapping from query nodes to Future objects representing the store tasks
    """
    ordered_list_of_queries = list(nx.topological_sort(dependency_graph))[::-1]
    logger.debug(f"Storing queries with IDs: {ordered_list_of_queries}")
    store_futures = {}
    for query in ordered_list_of_queries:
        try:
            store_futures[query] = dependency_graph.nodes[query]["query_object"].store()
        except UnstorableQueryError:
            # Some queries cannot be stored
            pass
    return store_futures


def store_all_unstored_dependencies(query_obj: "Query") -> None:
    """
    Store all of the unstored dependencies of a query.

    Parameters
    ----------
    query_obj : Query
        Query object whose dependencies will be stored.
    
    Notes
    -----
    This function stores only the unstored dependencies of a query, and not the
    query itself.
    This is a blocking function. Storing the dependencies happens in background threads,
    but this function will not return until all the dependencies are stored.
    """
    logger.debug(
        f"Creating background threads to store dependencies of query '{query_obj.query_id}'."
    )
    dependency_futures = store_queries_in_order(unstored_dependencies_graph(query_obj))

    logger.debug(f"Waiting for dependencies to finish executing...")
    wait(list(dependency_futures.values()))


def dependencies_eligible_for_store(query_obj: "Query") -> Set["Query"]:
    """
    Get the set of dependencies for this query which may be stored before it is run.

    Parameters
    ----------
    query_obj : Query
        Query object to get potentially eligible dependencies for

    Returns
    -------
    set of Query
        The set of dependencies of this query which may be stored before it is run.

    """
    logger.debug(f"Getting dependencies eligible for store for: {query_obj.query_id}")
    dependencies = set()

    openlist = set([query_obj])

    while openlist:
        x = openlist.pop()
        if not x.is_stored:
            dependencies.add(x)
            openlist.update(dep for dep in x.dependencies if dep not in dependencies)

    eligible = set()
    for query in dependencies:
        try:
            query.table_name
            eligible.add(query)
        except NotImplementedError:
            pass
    return eligible


def queued_dependencies(eligible_dependencies: Set["Query"]) -> List["Query"]:
    """
    Get the query objects from a set which are currently queued.

    Parameters
    ----------
    eligible_dependencies : set of Query
        Set of query objects that might be queued

    Returns
    -------
    list of Query
        List of query objects currently queued

    """
    return [
        qur for qur in eligible_dependencies if qur.query_state is QueryState.QUEUED
    ]


def executing_dependencies(eligible_dependencies: Set["Query"]) -> List["Query"]:
    """
    Get the query objects from a set which are currently executing.

    Parameters
    ----------
    eligible_dependencies : set of Query
        Set of query objects that might be executing

    Returns
    -------
    list of Query
        List of query objects currently executing

    """
    return [
        qur for qur in eligible_dependencies if qur.query_state is QueryState.EXECUTING
    ]


def query_progress(query: "Query") -> Dict[str, int]:
    """
    Check the progress of a query.

    Parameters
    ----------
    query : Query
        Query object to check progress of

    Returns
    -------
    dict
        eligible: Number of subqueries that must be run
        queued: number queued to be run
        executing: number currently running

    """
    if query.is_stored:
        return dict(eligible=0, queued=0, running=0)
    eligible = dependencies_eligible_for_store(query)
    queued = queued_dependencies(eligible)
    running = executing_dependencies(eligible)
    return dict(eligible=len(eligible), queued=len(queued), running=len(running))
