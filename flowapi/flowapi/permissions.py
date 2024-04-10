# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import asyncio
import collections
import functools
from typing import Any, Iterable

from prance import ResolvingParser
from quart import current_app, request
from rapidjson import dumps

from flowapi.flowapi_errors import (
    BadQueryError,
    JSONHTTPException,
    MissingQueryKindError,
)

try:
    logger = current_app.debug_logger
except RuntimeError:
    import structlog

    logger = structlog.get_logger(__name__)


@functools.singledispatch
def grab_on_key_list(in_iter, search_keys):
    """
    Searches `in_iter`, a deeply-nested set of dicts and/or lists forming a tree,
    for occurrences of `search_keys` nested directly within one another and
    yields the value contained by the final search key
    Parameters
    ----------
    in_iter : list or dict
        A deeply-nested set of lists or dicts forming a tree
    search_keys : list
        A list of str and/or ints to search for within the tree

    Yields
    -------
    An iterator of items at the end of search_keys

    Examples
    --------

    >>> input = {"1": [{}, {}, {"3": "success"}]}
    >>> keys = ["1", 2, "3"]
    >>> list(grab_on_key_list(input, keys))
    ["success"]

    >>> input = {"outer": {"not_inner": "wrong", "inner": "right"}}
    >>> keys = ["outer", "inner"]
    >>> list(grab_on_key_list(input, keys))
    ["right"]

    >>> input = {
    ...    "first": {"1": {"2": "first_inner"}},
    ...    "second": {"1": {"2": "second_inner"}},
    ...    "third": {"1": {"3": "not_needed"}},
    ... }
    >>> keys = ["1", "2"]
    >>> list(grab_on_key_list(input, keys))
    ["first_inner", "second_inner"]

    """
    # If passed anything that is not a list or dict, stop
    yield from ()


@grab_on_key_list.register
def _(in_iter: dict, search_keys: list):
    for key, value in in_iter.items():
        try:
            yield _search_for_nested_keys(in_iter, search_keys)
        except (KeyError, TypeError):
            pass
        yield from grab_on_key_list(value, search_keys)


@grab_on_key_list.register
def _(in_iter: list, search_keys: list):
    for value in in_iter:
        try:
            yield _search_for_nested_keys(value, search_keys)
        except (KeyError, TypeError):
            pass
        yield from grab_on_key_list(value, search_keys)


def _search_for_nested_keys(in_iter: dict, search_keys: Any) -> Any:
    out = in_iter
    for search_key in search_keys:
        out = out[search_key]
    return out


def schema_to_scopes(schema: dict) -> Iterable[str]:
    """
    Constructs and yields query scopes of the form:
    <agg_unit>:<tl_query>:<sub_query>
    where agg_unit is the name of an aggregation unit, <tl_query> is the query and <sub_query> is a dependent query.
    Every query yields a scope of the form <agg_unit>:<tl_query>:<tl_query>
    Additionally yields the "get_result&available_dates" scope.
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

    ["dummy", "run&dummy", "get_result&available_dates"],
    """
    resolved_queries = ResolvingParser(spec_string=dumps(schema)).specification[
        "components"
    ]["schemas"]["FlowmachineQuerySchema"]
    unique_scopes = set()
    for tl_query in resolved_queries["oneOf"]:
        tl_query_name = tl_query["properties"]["query_kind"]["enum"][0]
        logger.debug(f"Looking for {tl_query_name}")
        query_list = grab_on_key_list(
            tl_query,
            ["properties", "query_kind", "enum", 0],
        )

        scopes_generator = (
            tl_schema_scope_string(tl_query, query) for query in query_list
        )
        unique_scopes = set.union(unique_scopes, *scopes_generator)
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
        raise JSONHTTPException(description=reply["msg"])
    try:
        if reply["payload"]["aggregation_unit"] is None:
            return "nonspatial"
        else:
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

    Parameters
    ----------
    query_dict : dict
        Query to build the list of scopes from

    Raises
    ------
    MissingQueryKindError
        if "query_kind" is missing from `query_dict`
    BadQueryError
        If something else is wrong with `query_dict`
    """

    try:
        if "query_kind" not in query_dict.keys():
            raise MissingQueryKindError
        tl_query_name = query_dict["query_kind"]
        query_list = grab_on_key_list(query_dict, ["query_kind"])
    except Exception:
        raise BadQueryError
    agg_unit = await get_agg_unit(query_dict)
    return [f"{agg_unit}:{tl_query_name}:{query_name}" for query_name in query_list]


def tl_schema_scope_string(tl_query, query_string) -> set:
    """
    Given a top level (aggregate) query and a sub_query, return the scopes triplet for that query in
    the format 'geographic_area:top_level_query:sub_query'
    Parameters
    -----------
    tl_query : dict
        A query, as returned from `ResolvingParser`
    query_string : str
        The sub-query to build the scope for
    """
    out = set()
    tl_query_name = tl_query["properties"]["query_kind"]["enum"][0]
    try:
        agg_units = tl_query["properties"]["aggregation_unit"]["enum"]
    except KeyError:
        agg_units = ["nonspatial"]
    out = out | {f"{agg_unit}:{tl_query_name}:{query_string}" for agg_unit in agg_units}
    return out
