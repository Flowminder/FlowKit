# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import collections
import functools
from copy import deepcopy
from itertools import product
from typing import Iterable, List, Optional, Tuple, Union, Set, Any


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
