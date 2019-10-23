# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from itertools import takewhile, product, chain
from typing import List, Iterable, Set, FrozenSet, Tuple

from flowapi.flowapi_errors import MissingQueryKindError, BadQueryError


def get_nested_objects(schema: dict) -> Iterable[Tuple[str, dict]]:
    """

    Parameters
    ----------
    schema

    Returns
    -------

    """
    for q, qbod in schema.items():
        try:
            refs = [qref["$ref"].split("/")[-1] for qref in qbod["oneOf"]]
            yield q, refs
        except (KeyError, TypeError):
            pass  # Not a oneOf, or a malformed one


def get_nested_dict(schema: dict) -> dict:
    return dict(get_nested_objects(schema))


def get_queries(schema: dict) -> dict:
    """

    Parameters
    ----------
    schema

    Returns
    -------

    """
    return {
        q: qbod["properties"]
        for q, qbod in schema.items()
        if isinstance(qbod, dict) and "query_kind" in qbod.get("properties", {})
    }


def get_reffed_params(qs: dict, nested: dict) -> dict:
    """

    Parameters
    ----------
    qs
    nested

    Returns
    -------

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

    Parameters
    ----------
    roots
    q_treed

    Returns
    -------

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


def enum_paths(parents: List[str], tree: dict):
    """

    Parameters
    ----------
    parents
    tree

    Returns
    -------

    """
    if len(tree) == 0:
        yield parents, tree
    else:
        for k, v in tree.items():
            yield from enum_paths(parents + [k], v)


def make_per_query_scopes(tree: dict, all_queries: dict) -> Iterable[str]:
    """

    Parameters
    ----------
    tree
    all_queries

    Returns
    -------

    """
    units_superset = set()
    for path, _ in list(enum_paths([], tree)):
        kind_path = [
            all_queries.get(p, {}).get("query_kind", {}).get("enum", [p])[0]
            for p in path
        ]  # Want the snake-cased variant
        try:
            units = all_queries[path[-1]]["aggregation_unit"]["enum"]
            units_superset.update(units)
            yield from (
                ":".join(kind_path + ["aggregation_unit", unit]) for unit in units
            )
        except KeyError:
            yield ":".join(kind_path)
        except IndexError:
            continue
    yield from (
        ":".join(["geography", "aggregation_unit", unit])
        for unit in sorted(units_superset)
    )  # Special case for geography


def make_scopes(tree: dict, all_queries: dict) -> Iterable[str]:
    """

    Parameters
    ----------
    tree
    all_queries

    Returns
    -------

    """
    yield from (
        f"{action}:{scope}"
        for action in ("get_result", "run")
        for scope in make_per_query_scopes(tree, all_queries)
    )
    yield "get_result:available_dates"  # Special case for available dates


def schema_to_scopes(flowmachine_query_schemas: dict) -> Iterable[str]:
    """

    Parameters
    ----------
    flowmachine_query_schemas

    Returns
    -------

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

    Parameters
    ----------
    scope

    Returns
    -------

    """
    actions, query_kind, *scope = scope.split(":")
    scopeit = iter(scope)
    scope = takewhile(lambda x: x != "aggregation_unit", scopeit)
    scope_set_it = [list(scope)]
    try:
        agg_units = next(scopeit)
        scope_set_it = [
            [*scope_set_it[0], "aggregation_unit", unit]
            for unit in agg_units.split(",")
        ]
    except StopIteration:  # No aggregation units for this scope
        pass
    for action, scope in product(actions.split(","), scope_set_it):
        yield frozenset([action, query_kind, *zip(scope[::2], scope[1::2])])


def scopes_to_sets(scopes: List[str]) -> Set:
    """

    Parameters
    ----------
    scopes

    Returns
    -------

    """
    return set(chain.from_iterable(scope_to_sets(scope) for scope in scopes))


def query_to_scope_set(*, action: str, query: dict) -> Set:
    """

    Parameters
    ----------
    query

    Returns
    -------

    """
    try:
        ss = set([action, query["query_kind"]])
    except KeyError as exc:
        raise MissingQueryKindError
    except (AttributeError, TypeError) as exc:
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
