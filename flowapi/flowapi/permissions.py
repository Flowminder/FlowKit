# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from itertools import takewhile, product, chain
from typing import List, Iterable, Set, FrozenSet, Tuple

from flowapi.flowapi_errors import MissingQueryKindError, BadQueryError


def get_nested_objects(schema: dict) -> Iterable[Tuple[str, dict]]:
    """
    Yields tuples of nested objects as the name of the object and a list
    of the objects it references without the ref prefix.

    Parameters
    ----------
    schema : dict
        Schema to parse

    Yields
    ------
    tuple of str, dict

    """
    for q, qbod in schema.items():
        try:
            refs = [qref["$ref"].split("/")[-1] for qref in qbod["oneOf"]]
            yield q, refs
        except (KeyError, TypeError):
            pass  # Not a oneOf, or a malformed one


def get_nested_dict(schema: dict) -> dict:
    """
    Gets a dict containing nested objects with the name (sans ref)
    of the object they reference.

    Parameters
    ----------
    schema : dict
        Schema to parse

    Returns
    -------
    dict

    """
    return dict(get_nested_objects(schema))


def get_queries(schema: dict) -> dict:
    """
    Gets just query objects from a schema.

    Parameters
    ----------
    schema : dict

    Returns
    -------
    dict
        Dictionary of query objects
    """
    return {
        q: qbod["properties"]
        for q, qbod in schema.items()
        if isinstance(qbod, dict) and "query_kind" in qbod.get("properties", {})
    }


def get_reffed_params(qs: dict, nested: dict) -> dict:
    """
    Resolve references in query params.

    Parameters
    ----------
    qs : dict
        All mainline queries
    nested : dict
        All nested queries
    Returns
    -------
    dict
        Queries with any refs to another query substituted for that query

    """
    q_treed = {}
    for q, qbod in qs.items():
        q_treed[q] = {}
        for param, vals in qbod.items():
            if "$ref" in vals:
                ref = vals["$ref"].split("/")[-1]
                q_treed[q][param] = nested.get(ref, ref)
    return q_treed


def build_tree(roots: List[str], q_treed: dict) -> dict:
    """
    Given a list of roots, and a tree of queries with any references resolved, constructs n new trees
    with each combination of nested parameters as a branch, ensuring that the most branching occurs
    immediately under the root node.

    Parameters
    ----------
    roots : list of str
        List of query names to use as the roots of the tree
    q_treed : dict
        Tree of queries with any references resolved


    Returns
    -------
    dict
        The new tree

    Examples
    --------
    >>>build_tree(["DUMMY_ROOT"],{"DUMMY_ROOT": {"long_param": [1, 2, 3], "short_param": [1, 2]}})
    {"DUMMY_ROOT": {"long_param": {1: {"short_param": {1: {}, 2: {}}},2: {"short_param": {1: {}, 2: {}}},3: {"short_param": {1: {}, 2: {}}},}}}

    """
    tree = {}
    for root in roots:
        params = q_treed[root]
        tree[root] = {}
        refs = tree[root]
        sorted_params = sorted(params.items(), key=lambda x: (-len(x[1]), x[0]))
        try:
            longest_param, *others = sorted_params
            param_name, param_spec = longest_param
        except ValueError:
            continue
        tree[root][param_name] = {q_type: {} for q_type in param_spec}
        refs = refs[param_name]
        for param_name, param_spec in others:
            for q_type, q_bod in refs.items():
                refs[q_type][param_name] = {k: {} for k in param_spec}
    return tree


def enum_paths(parents: List[str], tree: dict) -> Tuple[List[str], dict]:
    """
    Yield the paths to the leaves of a tree and the associated leaf node.

    Parameters
    ----------
    parents : list of str
        Parents of this path
    tree : dict
        Tree of queries

    Yields
    ------
    Tuple of list, dict
    """
    if len(tree) == 0:
        yield parents, tree
    else:
        for k, v in tree.items():
            yield from enum_paths(parents + [k], v)


def make_per_query_scopes(tree: dict, all_queries: dict) -> Iterable[str]:
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
    >>>list(make_per_query_scopes({"DUMMY": {}}, {"DUMMY": {"query_kind": {"enum": ["dummy"]}}}))
    ["dummy"]
    >>>list(make_per_query_scopes({"DUMMY": {}},{"DUMMY": {"query_kind": {"enum": ["dummy"]},"aggregation_unit": {"enum": ["DUMMY_UNIT"]},}}))
    ["dummy:aggregation_unit:DUMMY_UNIT",]

    """
    for path, _ in list(enum_paths([], tree)):
        kind_path = [
            all_queries.get(p, {}).get("query_kind", {}).get("enum", [p])[0]
            for p in path
        ]  # Want the snake-cased variant
        try:
            units = all_queries[path[-1]]["aggregation_unit"]["enum"]
            yield from (
                ":".join(kind_path + ["aggregation_unit", unit]) for unit in units
            )
        except KeyError:
            yield ":".join(kind_path)
        except IndexError:
            continue


def make_scopes(tree: dict, all_queries: dict) -> Iterable[str]:
    """
    Constructs and yields query scopes of the form:
    <action>:<query_kind>:<arg_name>:<arg_val>
    where arg_val may be a query kind, or the name of an aggregation unit if applicable, and <action> is run or get_result.
    Additionally yields the "get_result:available_dates" scope.

    One scope is yielded for each viable query structure, so for queries which contain two child queries
    five scopes are yielded. If that query has 3 possible aggregation units, then 13 scopes are yielded altogether.

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
    >>>list(make_scopes({"DUMMY": {}}, {"DUMMY": {"query_kind": {"enum": ["dummy"]}}}))
    ["get_result:dummy", "run:dummy", "get_result:available_dates"]
    >>>list(make_scopes({"DUMMY": {}},{"DUMMY": {"query_kind": {"enum": ["dummy"]},"aggregation_unit": {"enum": ["DUMMY_UNIT"]},}}))
    ["run:dummy:aggregation_unit:DUMMY_UNIT","get_result:dummy:aggregation_unit:DUMMY_UNIT", "get_result:available_dates"]

    """
    yield from (
        f"{action}:{scope}"
        for action in ("get_result", "run")
        for scope in make_per_query_scopes(tree, all_queries)
    )
    yield "get_result:available_dates"  # Special case for available dates


def schema_to_scopes(flowmachine_query_schemas: dict) -> Iterable[str]:
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
    yield from make_scopes(
        build_tree(
            get_nested_dict(flowmachine_query_schemas)["FlowmachineQuerySchema"],
            get_reffed_params(
                get_queries(flowmachine_query_schemas),
                get_nested_dict(flowmachine_query_schemas),
            ),
        ),
        get_queries(flowmachine_query_schemas),
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
