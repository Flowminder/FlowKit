# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from itertools import product, chain, repeat
from prance import ResolvingParser
from rapidjson import dumps
from typing import List, Iterable, Set, FrozenSet, Tuple, Optional, Union, Any

from flowapi.flowapi_errors import MissingQueryKindError, BadQueryError


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


def reparent(*, tree, bubble_down=None, depth=1):
    if bubble_down is None:
        bubble_down = []
    if isinstance(tree, list) or len(tree) == 0:
        # At a leaf
        if len(bubble_down) > 0:
            materialise_now, *bubble_down = bubble_down
            if len(tree) == 0:
                return {
                    materialise_now[0]: reparent(
                        tree=materialise_now[1],
                        bubble_down=bubble_down,
                        depth=depth + 1,
                    )
                }
            return {
                k: reparent(
                    tree=dict([materialise_now]),
                    bubble_down=bubble_down,
                    depth=depth + 1,
                )
                for k in tree
            }
        return {k: dict() for k in tree}
    if depth % 2 == 0:
        return {
            k: reparent(tree=v, bubble_down=bubble_down, depth=depth + 1)
            for k, v in tree.items()
        }
    else:
        # Sort the arguments by their number of values
        sorted_items = sorted(tree.items(), key=lambda x: (-len(x[1]), x[0]))
        longest, *rest = sorted_items
        # Put the other items under the longest
        key, val = longest
        return {
            key: reparent(tree=val, bubble_down=rest + bubble_down, depth=depth + 1)
        }


def walk_tree(*, tree: dict, paths: Optional[List[str]] = None):
    if paths is None:
        paths = []
    if len(tree) == 0:
        yield paths, tree
    else:
        for k, v in tree.items():
            yield from walk_tree(paths=paths + [k], tree=v)


def make_per_query_scopes(
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
        ":".join([q] + x)
        for q, q_bod in nested_qs.items()
        for x, y in walk_tree(tree=reparent(tree=q_bod))
    )


def make_scopes(
    *, queries: dict, argument_names_to_extract: List[str] = ["aggregation_unit"]
) -> Iterable[str]:
    """
    Constructs and yields query scopes of the form:
    <action>:<query_kind>:<arg_name>:<arg_val>
    where arg_val may be a query kind, or the name of an aggregation unit if applicable, and <action> is run or get_result.
    Additionally yields the "get_result:available_dates" scope.

    One scope is yielded for each viable query structure, so for queries which contain two child queries
    five scopes are yielded. If that query has 3 possible aggregation units, then 13 scopes are yielded altogether.

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
    >>>list(make_scopes(queries={"DUMMY": {}}))
    ["get_result:dummy", "run:dummy", "get_result:available_dates"]
    >>>list(make_scopes(queries={"DUMMY": {}}))
    ["run:dummy:aggregation_unit:DUMMY_UNIT","get_result:dummy:aggregation_unit:DUMMY_UNIT", "get_result:available_dates"]

    """
    return [
        ":".join(x)
        for x in product(
            ("get_result", "run"),
            make_per_query_scopes(
                queries=queries, argument_names_to_extract=argument_names_to_extract
            ),
        )
    ] + [
        "get_result:available_dates"
    ]  # Special case for available dates


def schema_to_scopes(schema: dict) -> Iterable[str]:
    """
    Constructs and yields query scopes of the form:
    <action>:<query_kind>:<arg_name>:<arg_val>
    where arg_val may be a query kind, or the name of an aggregation unit if applicable, and <action> is run or get_result.
    Additionally yields the "get_result:available_dates" scope.

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
    ["get_result:dummy", "run:dummy", "get_result:available_dates"],
    """
    return make_scopes(
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
        parts = scope.split(":")
        ps = (x.split(",") for x in parts)
        yield from (":".join(x) for x in product(*ps))


def q_to_scope_atoms(
    *, query: dict, argument_names_to_extract: List[str] = ["aggregation_unit"],
) -> List[Union[str, List[str]]]:
    """
    Convert a query to scope 'atoms', each of which would be a contiguous
    part of a scope string.

    Parameters
    ----------
    query : dict
        Query to atomise
    argument_names_to_extract : list of str
        Arguments to pull out

    Returns
    -------
    list of str
       Nested list of atoms
    """
    paths = []
    if "query_kind" in query:
        paths = paths + [query["query_kind"]]
    for k, v in sorted(query.items()):
        if k in argument_names_to_extract:
            paths = paths + [k, v]
        if isinstance(v, dict):
            atoms = q_to_scope_atoms(query=v)
            if len(atoms) > 0:
                paths = paths + [[k, *atoms]]
        if isinstance(v, list):
            child_paths = set()
            for x in v:
                if isinstance(x, dict):
                    child_paths.add(tuple(q_to_scope_atoms(query=x)))
            for child in child_paths:
                if len(child) > 0:
                    paths = paths + [k] + list(child)
    return paths


def q_to_subscopes(
    *, query: dict, argument_names_to_extract: List[str] = ["aggregation_unit"],
):
    """
    Translate a query into contiguous 'subscopes', which when combined in
    the correct order form a complete scope.

    Parameters
    ----------
    query : dict
        Query to translate to subscopes
     argument_names_to_extract : list of str
        Arguments to pull out
    Returns
    -------
    list of str
        List of subscopes

    """
    atoms = q_to_scope_atoms(
        query=query, argument_names_to_extract=argument_names_to_extract
    )
    top_level = [x for x in atoms if isinstance(x, str)]
    return [":".join(top_level)] + [
        ":".join(flatten(x)) for x in atoms if isinstance(x, list)
    ]


def flatten(container: Any, container_types: Tuple[type] = (list, tuple, set)) -> list:
    """
    String safe nested list flattener.

    Parameters
    ----------
    container : any
        Generally a nested list, will be flattened preserving any strings.
    container_types : tuple of type, default (list, tuple, set)
        Types to treat as containers

    Yields
    ------
    list
        Items from the flattened list or the original item if not a container
    """
    if isinstance(container, container_types):
        for i in container:
            if isinstance(i, container_types):
                yield from flatten(i)
            else:
                yield i
    else:
        yield container
