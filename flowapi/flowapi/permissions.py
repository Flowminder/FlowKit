# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import functools
import itertools
from itertools import product, repeat
from typing import Iterable, List, Optional, Tuple, Union

from prance import ResolvingParser
import jmespath
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
        and not (
            ("readOnly" in tree.keys()) and tree["readOnly"]
        )  # Workaround - read-only aggregation units will be required for new permissions, but should not be taken into account for current permissions
    ):
        yield (new_path, f"{{{new_path[-1]}}}")
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


def dict_and_list_key_function(
    x: Tuple[str, Union[dict, list, tuple]],
) -> Union[Tuple[bool, int, str], Tuple[bool, str]]:
    """
    Key function which sorts itemviews. Sorts items where the value is
    a list or a dict _after_ those where it isn't, then sorts dicts and
    lists by their length, then by the key.


    Parameters
    ----------
    x : tuple
        Item to get a key for.

    Returns
    -------
    tuple of bool, int and input or tuple of bool and input
        False, negative length of input and then key for dicts and lists
        True, key for other tuples

    """
    if isinstance(x[1], (dict, list)):
        return False, -len(x[1]), x[0]
    else:
        return True, x[0]


@functools.singledispatch
def valid_tree_walks(
    tree,
    *,
    paths: Optional[Iterable[str]] = None,
    depth: int = 1,
) -> Union[tuple, list]:
    """

    Parameters
    ----------
    tree : list of str or dict
        Tree to walk through
    paths : iterable of str or None
        Nodes already traversed
    depth : int, default 1
        Current depth in the tree


    Yields
    ------
    list of str or tuple of list of str
        Path through the tree

    """
    yield [*paths, tree]


@valid_tree_walks.register
def _(
    tree: list,
    *,
    paths: Optional[Iterable[str]] = None,
    depth: int = 1,
) -> Union[tuple, list]:
    if paths is None:
        paths = tuple()
    if len(tree) == 0 and len(paths) > 0:
        yield list(paths)
    else:
        for v in tree:
            yield [*paths, v]


@valid_tree_walks.register
def _(
    tree: dict,
    *,
    paths: Optional[Iterable[str]] = None,
    depth: int = 1,
) -> Union[tuple, list]:
    even_depth = depth % 2 == 0
    if paths is None:
        paths = tuple()
    if len(tree) == 0 and len(paths) > 0:
        yield list(paths)
    elif not even_depth or len(tree) == 1:
        for k, v in sorted(tree.items(), key=dict_and_list_key_function):
            yield from valid_tree_walks(v, paths=(*paths, k), depth=depth + 1)
    else:
        yield from product(
            *(
                valid_tree_walks(v, paths=(*paths, k), depth=depth + 1)
                for k, v in sorted(
                    tree.items(),
                    key=dict_and_list_key_function,
                )
            )
        )


@functools.singledispatch
def tree_walk_to_scope_list(tree_walk) -> str:
    """

    Parameters
    ----------
    tree_walk : str, list or str or tuple of list of str
        A path through the tree

    Yields
    ------
    str
        The tree walk as a . delimited string

    """
    yield tree_walk


@tree_walk_to_scope_list.register
def _(tree_walk: list) -> str:
    yield ".".join(tree_walk)


@tree_walk_to_scope_list.register
def _(tree_walk: tuple) -> str:
    for walk in tree_walk:
        yield from tree_walk_to_scope_list(walk)


def is_flat(in_iter):
    """
    Returns True if in_iter is flat (contains no dicts or lists)
    """
    if type(in_iter) is dict:
        in_iter = in_iter.values()
    return all(type(item) not in [dict, list] for item in in_iter)


@functools.singledispatch
def _flatten_inner(root, running_list):
    # This should be overridden by the registered overwrites
    raise TypeError("_flatten_inner should only take dict or list types")


@_flatten_inner.register(dict)
def _(root: dict, running_list: list, key_of_interest=None, _=None):
    for node, value in root.items():
        if is_flat(value):
            running_list.append(value)
        else:
            yield from _flatten_inner(value, running_list)


@_flatten_inner.register(list)
def _(root: list, running_list: list, _, __):
    for node in root:
        if is_flat(node):
            running_list.append(node)
        else:
            yield from _flatten_inner(node, running_list)


@functools.singledispatch
def _flatten_on_key_inner(root, key_of_interest):
    raise TypeError


@_flatten_on_key_inner.register(dict)
def _(root, key_of_interest):
    for node, value in root.items():
        if is_flat(value):
            pass
        else:
            yield from _flatten_on_key_inner(value, key_of_interest)
            if node == key_of_interest:
                # We cannot change the size of a dict mid-iterate, so instead we mark it for
                # deletion post-iterate
                root[node] = {}
                yield value


@_flatten_on_key_inner.register(list)
def _(root, key_of_interest):
    for value in root:
        yield from _flatten_on_key_inner(value, key_of_interest)


def flatten(in_iter):
    """
    When provided with a collection that contains other collections, returns a
    list of the innermost dictionaries or elements of that collection
    """
    out_list = []

    try:
        next(_flatten_inner(in_iter, out_list))
    except StopIteration:
        pass
    return out_list


def _clean_empties(in_dict, marker):
    out = {}
    for key, value in in_dict.items():
        if value != {marker: {}}:
            out[key] = value
    return out


def flatten_on_key(in_iter, key):
    out = list(_flatten_on_key_inner(in_iter, key))
    clean_out = [_clean_empties(flattened, key) for flattened in out]
    return clean_out


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
    # What we want here;
    # 1: Find every 'properties' node in the tree
    # 2: Extract the query_type from each property
    # 3: If present, extract the 'aggregation_unit' from each property

    def child_queries(in_dict: dict) -> Union[dict, None]:
        try:
            return in_dict["properties"]
        except KeyError:
            return None

    def has_properties(in_dict: dict) -> bool:
        return "properties" in in_dict.keys()

    # I wish I had a pen and paper
    # OK, let's think this through
    # We have a dict-of-lists-of-dicts-of-dicts, to a finite but unknown depth
    # For top dict: Look through values, get values that have type 'dict'
    # Glue that into the accumulator list
    # For each value that is type 'dict', pass to the next recursion

    def flatten_tree(tree):
        acc_dict = []
        tree = walk_tree(tree, acc_dict)
        try:
            _, acc_dict = next(tree)
        except StopIteration:
            pass
        return acc_dict

    queries = flatten_tree(queries)

    return queries

    # yield from (
    #     "&".join([action, *tree_walk_to_scope_list(scope_list)])
    #     for scope_list in valid_tree_walks(nested_qs)
    #     for action in ("get_result", "run")
    # )
    # yield "get_result&available_dates"


# This is the one that's causing the issue.
# At present, it yields
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
    >>> list(schema_to_scopes({"FlowmachineQuerySchema": {"oneOf": [{"$ref": "DUMMY"}]},"DUMMY": {"properties": {"query_kind": {"enum": ["dummy"]}}},},))
    ["get_result&dummy", "run&dummy", "get_result&available_dates"],
    """

    # Note from meeting; this will need to be per-role check, as all permissions for a query have to be contained in
    # a single role

    # Example query scopes:
    # "run",
    #  "read",
    #  "spatial_aggregate",
    #  "locations:admin_1",
    #  "locations:admin_3",
    #  "event_dates:1990-02-01:1992-03-04"
    #  "event_type:mds",
    #  "event_type:sms",
    #  "subscriber_subset"

    # Boolean permissions:
    # Check run
    # Check read
    # Check subscriber subset
    # Check event types
    # Check query tree
    # Check dates

    query_list = flatten_on_key(schema, "properties")
    if query_list == []:
        return []
    out = list(set.union(*(scopes_from_query(query) for query in query_list)))
    return sorted(out)


def scopes_from_query(query) -> set:
    query_kind = query["query_kind"]["enum"][0]
    out = {query_kind}
    try:
        agg_units = query["aggregation_unit"]["enum"]
        out = out | {":".join([query_kind, agg_unit]) for agg_unit in agg_units}
    except KeyError:
        pass
    return out


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
        parts = scope.strip().split("&")
        ps = (x.split(",") for x in parts)
        yield from (set(x) for x in product(*ps))


@functools.singledispatch
def query_to_scope_list(tree, paths=None, keep=["aggregation_unit"]) -> str:
    """

    Parameters
    ----------
    tree : list of dict or dict
    paths : tuple
    keep : list of str
        List of fields to include in scope strings

    Yields
    ------
    str
        Scope string


    """
    yield from ()


@query_to_scope_list.register
def _(tree: list, paths=None, keep=["aggregation_unit"]) -> str:
    if paths is None:
        paths = tuple()
    for v in tree:
        yield from query_to_scope_list(v, paths, keep=keep)


@query_to_scope_list.register
def _(tree: dict, paths=None, keep=["aggregation_unit"]) -> str:
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
