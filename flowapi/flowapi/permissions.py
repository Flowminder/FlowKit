# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from itertools import product, chain, repeat
from prance import ResolvingParser
from rapidjson import dumps
from typing import List, Iterable, Set, FrozenSet, Tuple, Optional

from flowapi.flowapi_errors import MissingQueryKindError, BadQueryError


def enum_paths(
    *,
    tree: dict,
    paths: Optional[List[str]] = None,
    argument_names_to_extract: List[str] = ["aggregation_unit"]
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


def scope_to_sets(scope: str) -> Iterable[FrozenSet]:
    """
    Translates a scope string into a set of the form {<action>, <query_kind>, (<arg_name>, <arg_value>)}

    Parameters
    ----------
    scope : str
        Scope string of the form <action>:<query_kind>:<arg_name>:<arg_value>

    Yields
    ------
    frozenset

    """
    parts = scope.split(":")
    ps = [x.split(",") for x in parts]
    for scope in product(*ps):
        scopeit = iter(scope)
        action, query_kind, *rest = scopeit
        if len(rest) > 0:
            yield frozenset([action, query_kind, *zip(rest[::2], rest[1::2])])
        else:
            yield frozenset([action, query_kind])


def scopes_to_sets(scopes: List[str]) -> Set:
    """
    Translates a list of scope strings into sets of the form {<action>, <query_kind>, (<arg_name>, <arg_value>)}

    Parameters
    ----------
    scopes : list of str
        Scope strings of the form <action>:<query_kind>:<arg_name>:<arg_value>
    Yields
    ------
    frozenset

    """
    return set(chain.from_iterable(scope_to_sets(scope) for scope in scopes))


def query_to_scope_set(*, action: str, query: dict) -> Set:
    """
    Translates a query request into a set of the form {<action>, <query_kind>, (<arg_name>, <arg_value>)}.

    Parameters
    ----------
    action : {"run", "get_result"}
        Action being done with this query.
    query : dict
        The query parameters.

    Returns
    -------
    set
       Set of the form {<action>, <query_kind>, (<arg_name>, <arg_value>)}

    """
    try:
        ss = set([action, query["query_kind"]])
    except KeyError:
        raise MissingQueryKindError
    except (AttributeError, TypeError):
        raise BadQueryError

    if "aggregation_unit" in query:
        ss.add(("aggregation_unit", query["aggregation_unit"]))
    child_queries = ((k, v) for k, v in query.items() if isinstance(v, dict))
    for k, v in child_queries:
        if "query_kind" in v:
            ss.add((k, v["query_kind"]))
        if "aggregation_unit" in v:
            ss.add(("aggregation_unit", v["aggregation_unit"]))
    return ss
