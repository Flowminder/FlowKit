# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import collections
import functools
from copy import deepcopy
from itertools import product
from typing import Iterable, List, Optional, Tuple, Union, Set, Any


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
    if not isinstance(in_iter, collections.Container):
        return True
    if isinstance(in_iter, dict):
        in_iter = in_iter.values()
    # Think there's a slightly better way of doing type introspection here
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


def flatten_on_key(in_iter, key, _in_place=False):
    if not _in_place:
        in_iter = deepcopy(in_iter)
    out = list(_flatten_on_key_inner(in_iter, key))
    clean_out = [_clean_empties(flattened, key) for flattened in out]
    return clean_out


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

    # A note here: flatten_on_key _will_ affect schema. This doesn't seem like
    # an issue right now, but may add a copy()
    query_list = flatten_on_key(schema, "properties")
    if query_list == []:
        return []
    scopes_generator = (scopes_from_query(query) for query in query_list)
    unique_scopes = list(set.union(*scopes_generator))
    # When do we add on the run/read scope?
    return sorted(unique_scopes)


def scopes_from_query(query) -> set:
    """
    Given a single query, returns the scopes needed for that query
    only
    """
    try:
        query_kind = query["query_kind"]["enum"][0]
    except KeyError:
        return set()
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
