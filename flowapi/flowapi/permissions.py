# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import functools
from itertools import chain, product, repeat
from typing import Any, FrozenSet, Iterable, List, Optional, Set, Tuple, Union

from flowapi.flowapi_errors import BadQueryError, MissingQueryKindError
from prance import ResolvingParser
from rapidjson import dumps


def enum_paths(
    *,
    tree: dict,
    paths: Optional[List[str]] = None,
    argument_names_to_extract: List[str] = ["aggregation_unit"],
) -> Tuple[List[str], dict]:
    """
    Yield the paths to the leaves of a tree and the associated leaf node.

    Parameters
    ----------
    *
    paths : list of str
        Parents of this path
    tree : dict
        Tree of queries

    Yields
    ------
    Tuple of list, dict
    """
    if paths is None:
        paths = []
    new_path = list(paths)
    if "properties" in tree and "query_kind" in tree["properties"]:
        new_path = paths + tree["properties"]["query_kind"]["enum"]
    if "oneOf" in tree.keys():  # Path divergence
        for tr in tree["oneOf"]:
            yield from enum_paths(paths=new_path, tree=tr)
    elif (
        "enum" in tree.keys()
        and len(tree["enum"]) > 1
        and new_path[-1] in argument_names_to_extract
    ):
        yield from zip(repeat(new_path), tree["enum"])
    elif "items" in tree.keys():
        yield from enum_paths(paths=new_path, tree=tree["items"])
    else:
        if "properties" in tree:
            for k, v in tree["properties"].items():
                if k == "query_kind":
                    yield new_path,
                else:
                    yield from enum_paths(paths=new_path + [k], tree=v)


def paths_to_nested_dict(
    *, queries: dict, argument_names_to_extract: List[str] = ["aggregation_unit"]
) -> dict:
    d = {}
    for x in list(
        enum_paths(tree=queries, argument_names_to_extract=argument_names_to_extract)
    ):
        path, *key = x
        the_d = d
        for v in path[:-1]:
            the_d = the_d.setdefault(v, dict())
        if len(key) > 0:
            the_d.setdefault(path[-1], list()).append(key[0])
        else:
            the_d.setdefault(path[-1], dict())
    return d


def valid_tree_walks(*, tree, paths=None, depth=1):
    def sort_func(x):
        if isinstance(x[1], (dict, list)):
            return False, -len(x[1]), x[0]
        else:
            return True, x[0]

    even_depth = depth % 2 == 0
    if paths is None:
        paths = tuple()
    if len(tree) == 0 and len(paths) > 0:
        yield list(paths)
    elif hasattr(tree, "items") and (not even_depth or len(tree) == 1):
        for k, v in sorted(tree.items(), key=sort_func):
            yield from valid_tree_walks(paths=(*paths, k), tree=v, depth=depth + 1)
    elif hasattr(tree, "items"):
        yield from product(
            *(
                valid_tree_walks(paths=(*paths, k), tree=v, depth=depth + 1)
                for k, v in sorted(tree.items(), key=sort_func,)
            )
        )
    else:
        for v in tree:
            yield [*paths, v]


def tree_walk_to_scope_list(tree_walk: Union[str, List[str], Tuple[List[str]]]):
    if isinstance(tree_walk, str):
        yield tree_walk
    if isinstance(tree_walk, list):
        yield ".".join(tree_walk)
    elif isinstance(tree_walk, tuple):
        for walk in tree_walk:
            yield from tree_walk_to_scope_list(walk)


def per_query_scopes(
    *, queries: dict, argument_names_to_extract: List[str] = ["aggregation_unit"]
) -> Iterable[str]:
    """
    Constructs and yields query scopes of the form:
    <query_kind>:<arg_name>:<arg_val>
    where arg_val may be a query kind, or the name of an aggregation unit if applicable.

    One scope is yielded for each viable query structure, so for queries which contain two child queries
    two scopes are yielded. If that query has 3 possible aggregation units, then 6 scopes are yielded altogether.

    Parameters
    ----------
    *
    tree : dict
        Dict of nested queries
    all_queries : dict
        All queries

    Yields
    ------
    str
        Query scope of the form <query_kind>:<arg_name>:<arg_val>

    Examples
    --------
    >>>list(make_per_query_scopes(queries={"DUMMY": {}}))
    ["dummy"]
    >>>list(make_per_query_scopes(queries={"DUMMY": {}}))
    ["dummy:aggregation_unit:DUMMY_UNIT",]

    """
    nested_qs = paths_to_nested_dict(
        queries=queries, argument_names_to_extract=argument_names_to_extract
    )
    yield from (
        "&".join([action, *tree_walk_to_scope_list(scope_list)])
        for scope_list in valid_tree_walks(tree=nested_qs)
        for action in ("get_result", "run")
    )
    yield "get_result&available_dates"


def schema_to_scopes(schema: dict) -> Iterable[str]:
    """
    Constructs and yields query scopes of the form:
    <action>:<query_kind>:<arg_name>:<arg_val>
    where arg_val may be a query kind, or the name of an aggregation unit if applicable, and <action> is run or get_result.
    Additionally yields the "get_result&available_dates" scope.

    One scope is yielded for each viable query structure, so for queries which contain two child queries
    five scopes are yielded. If that query has 3 possible aggregation units, then 13 scopes are yielded altogether.

    Parameters
    ----------
    flowmachine_query_schemas : dict
        Schema dict to turn into scopes list

    Yields
    ------
    str
        Scope strings

    Examples
    --------
    >>>list(schema_to_scopes({"FlowmachineQuerySchema": {"oneOf": [{"$ref": "DUMMY"}]},"DUMMY": {"properties": {"query_kind": {"enum": ["dummy"]}}},},))
    ["get_result&dummy", "run&dummy", "get_result&available_dates"],
    """
    yield from per_query_scopes(
        queries=ResolvingParser(spec_string=dumps(schema)).specification["components"][
            "schemas"
        ]["FlowmachineQuerySchema"]
    )


def expand_scopes(*, scopes: List[str]) -> str:
    """
    Expand up a list of compact scopes to full scopes

    Parameters
    ----------
    scopes : list of str
        Compressed scopes to expand

    Yields
    ------
    str
        A scope string

    """
    for scope in scopes:
        parts = scope.split("&")
        ps = (x.split(",") for x in parts)
        yield from (set(x) for x in product(*ps))


@functools.singledispatch
def query_to_scope_list(tree, paths=None, keep=["aggregation_unit"]):
    yield from ()


@query_to_scope_list.register
def _(tree: list, paths=None, keep=["aggregation_unit"]):
    if paths is None:
        paths = tuple()
    for v in tree:
        yield from query_to_scope_list(v, paths, keep=keep)


@query_to_scope_list.register
def _(tree: dict, paths=None, keep=["aggregation_unit"]):
    if paths is None:
        paths = tuple()
    try:
        q_kind = tree["query_kind"]
        paths = (*paths, q_kind)
    except KeyError:
        pass

    yielded_any = False
    for k, v in sorted(tree.items(), key=lambda x: x[0]):
        if k == "query_kind":
            continue
        if k in keep:
            yield ".".join((*paths, k, v))
            yielded_any = True
        elif isinstance(v, (dict, list)):
            for t in query_to_scope_list(v, (*paths, k), keep=keep):
                yield t
                yielded_any = True
    if not yielded_any and "query_kind" in tree:
        yield ".".join(paths)
