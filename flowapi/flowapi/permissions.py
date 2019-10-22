# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


from typing import List, Iterable, Set


def get_nested_objects(schema: dict) -> dict:
    return {
        q: [qref["$ref"].split("/")[-1] for qref in qbod["oneOf"]]
        for q, qbod in schema.items()
        if "oneOf" in qbod
    }


def get_queries(schema: dict) -> dict:
    return {
        q: qbod["properties"]
        for q, qbod in schema.items()
        if "query_kind" in qbod.get("properties", {})
    }


def get_reffed_params(qs: dict, nested: dict) -> dict:
    q_treed = {}
    for q, qbod in qs.items():
        q_treed[q] = {}
        for param, vals in qbod.items():
            if "$ref" in vals:
                ref = vals["$ref"].split("/")[-1]
                q_treed[q][param] = nested.get(ref, ref)
    return q_treed


def build_tree(roots: List[str], q_treed: dict) -> dict:
    tree = {}
    for r in roots:
        params = q_treed[r]
        tree[r] = {}
        refs = tree[r]
        try:
            sorted_params = sorted(params.items(), key=lambda x: -len(x[1]))
            longest_param, *others = sorted_params
        except ValueError:  # Only one param
            try:
                longest_param = sorted_params[0]
                others = []
            except IndexError:
                continue
        param_name, param_spec = longest_param
        tree[r][param_name] = {q_type: {} for q_type in param_spec}
        refs = refs[param_name]
        for param_name, param_spec in others:
            for q_type, q_bod in refs.items():
                refs[q_type][param_name] = {k: {} for k in param_spec}
    return tree


def enum_paths(parents: List[str], tree: dict):
    if len(tree) == 0:
        yield parents, tree
    else:
        for k, v in tree.items():
            yield from enum_paths(parents + [k], v)


def make_per_query_scopes(tree: dict, all_queries: dict) -> Iterable[str]:
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


def make_scopes(tree: dict, all_queries: dict) -> Iterable[str]:
    yield from (
        f"{action}:{scope}"
        for action in ("read", "write")
        for scope in make_per_query_scopes(tree, all_queries)
    )


def schema_to_scopes(flowmachine_query_schemas: dict) -> Iterable[str]:
    yield from make_scopes(
        build_tree(
            get_nested_objects(flowmachine_query_schemas)["FlowmachineQuerySchema"],
            get_reffed_params(
                get_queries(flowmachine_query_schemas),
                get_nested_objects(flowmachine_query_schemas),
            ),
        ),
        get_queries(flowmachine_query_schemas),
    )


def scopes_to_sets(scopes: List[str]) -> Set:
    scopesets = set()
    for scope in scopes:
        action, query_kind, *scope = scope.split(":")
        scopesets.add(frozenset([action, query_kind, *zip(scope[::2], scope[1::2])]))
    return scopesets


def query_to_scope_set(query: dict) -> Set:
    ss = set([query["action"], query["query_kind"]])
    if "aggregation_unit" in query:
        ss.add(("aggregation_unit", query["aggregation_unit"]))
    for k, v in query.items():
        if "query_kind" in v:
            ss.add((k, v["query_kind"]))
        if "aggregation_unit" in v:
            ss.add(("aggregation_unit", v["aggregation_unit"]))
    return ss
