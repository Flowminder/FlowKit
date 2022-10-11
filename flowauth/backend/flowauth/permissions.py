# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import asyncio
import collections
import functools
import pdb
from copy import deepcopy
from itertools import product
from typing import Iterable, List, Optional, Tuple, Union, Set, Any
from prance import ResolvingParser
from rapidjson import dumps

import logging


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
def _(
    root,
    key_of_interest,
):
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
def _(
    root,
    key_of_interest,
):
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
    clean_out = list(_clean_empties(flattened, key) for flattened in out)
    return clean_out


def grab_on_key_list(in_iter, keys):
    """
    Looks through the iterator and yields every value at the end of the chain of keys
    :param in_iter:
    :param key:
    :return:
    """
    # I'm not a fan of the mutate-passed-in-list approach; it feels like
    # it's going against the philosophy of functional programmign, as it
    # exploits a side-effect. But it works, so....
    out_list = []
    iter = _grab_on_key_list_inner(in_iter, keys, out_list)
    try:
        next(iter)
    except StopIteration:
        pass
    return out_list


@functools.singledispatch
def _grab_on_key_list_inner(in_iter, search_keys, results):
    # If passed anything that is not a list or dict, pass
    pass


@_grab_on_key_list_inner.register(dict)
def _(in_iter, search_keys, results):
    for key, value in in_iter.items():
        if key == search_keys[0]:
            out = _seach_for_nested_keys(in_iter, search_keys)
            if out:
                results.append(out)
        if type(value) in [list, dict]:
            yield from _grab_on_key_list_inner(value, search_keys, results)


def _seach_for_nested_keys(in_iter, search_keys):
    out = in_iter
    try:
        for search_key in search_keys:
            out = out[search_key]
        return out
    except KeyError:
        return None


@_grab_on_key_list_inner.register(list)
def _(in_iter, search_keys, results):
    for value in in_iter:
        if value == search_keys[0]:
            out = _seach_for_nested_keys(value, search_keys)
            if out:
                results.append(out)
        if type(value) in [list, dict]:
            yield from _grab_on_key_list_inner(value, search_keys, results)


def schema_to_scopes(schema: dict) -> Iterable[str]:
    """
    Constructs and yields query scopes of the form:
    <agg_unit>:<tl_query>:<sub_query>
    where agg_unit is the name of an aggregation unit, <tl_query> is the query and <sub_query> is a dependent query.
    Every query yields a scope of the form <agg_unit>:<tl_query>:<tl_query>
    One scope is yielded for each descendent of TL query, so for queries which contain two child queries
    three scopes are yielded. If that query has 3 possible aggregation units, then 9 scopes are yielded altogether.

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

    >>> list(
        schema_to_scopes(
            {
                "FlowmachineQuerySchema": {
                    "oneOf": [{"$ref": "DUMMY"}]},
                "DUMMY": {"properties": {"query_kind": {"enum": ["dummy"]}}},
            },
        )
    )

    ["nonspatial:dummy:dummy"],
    """
    resolved_queries = ResolvingParser(spec_string=dumps(schema)).specification[
        "components"
    ]["schemas"]["FlowmachineQuerySchema"]
    unique_scopes = []
    for tl_query in resolved_queries["oneOf"]:
        tl_query_name = tl_query["properties"]["query_kind"]["enum"][0]
        print(f"Looking for {tl_query_name}")
        query_list = grab_on_key_list(
            tl_query,
            ["properties", "query_kind", "enum", 0],
        )
        if query_list == []:
            return []
        scopes_generator = (
            tl_schema_scope_string(tl_query, query) for query in query_list
        )
        unique_scopes += list(set.union(*scopes_generator))
    return sorted(unique_scopes)


async def get_agg_unit(query_dict):
    """
    Interrogates Flowmachine for the top-level agg unit of this query
    """
    request.socket.send_json(
        {
            "request_id": request.request_id,
            "action": "get_aggregation_unit",
            "params": query_dict,
        }
    )
    reply = await request.socket.recv_json()
    if reply["status"] != "success":
        raise Exception("Query has no type - something wrong with Flowmachine")
    try:
        return reply["payload"]["aggregation_unit"]
    except KeyError:
        raise Exception(
            "Reply missing aggregation_unit key - something wrong with Flowmachine."
            f"Received output: {reply}"
        )


async def query_to_scopes(query_dict):
    """
    Given a query_dict of the form
    {
        query_kind:tl_query,
        aggregation_unit:agg_unit
        ...
        sub_param:{
            query_kind: sub_query...}
    }
    returns the scope triplets of the query in the form "agg_unit:tl_query:sub_query".
    Will always return "agg_unit:tl_query:tl_query"
    :param query_dict:
    :return:
    """
    tl_query_name = query_dict["query_kind"]
    query_list = grab_on_key_list(query_dict, ["query_kind"])
    agg_unit = await get_agg_unit(query_dict)
    return [f"{agg_unit}:{tl_query_name}:{query_name}" for query_name in query_list]


def tl_schema_scope_string(tl_query, query_string) -> set:
    """
    Given a top level (aggregate) query and a sub_query, return the scopes triplet for that query in
    the format 'geographic_area:top_level_query:sub_query'
    :param tl_query:
    """
    out = set()
    tl_query_name = tl_query["properties"]["query_kind"]["enum"][0]
    try:
        agg_units = tl_query["properties"]["aggregation_unit"]["enum"]
    except KeyError:
        current_app.flowapi_logger.warning(
            f"No aggregation unit options for {tl_query_name}; "
            f"defauting to 'nonspatial'"
        )
        agg_units = ["nonspatial"]
    out = out | {f"{agg_unit}:{tl_query_name}:{query_string}" for agg_unit in agg_units}
    return out
