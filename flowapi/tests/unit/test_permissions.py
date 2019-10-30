# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import pytest

from flowapi.permissions import (
    get_nested_dict,
    get_queries,
    get_reffed_params,
    build_tree,
    enum_paths,
    make_per_query_scopes,
    make_scopes,
    schema_to_scopes,
)


@pytest.mark.parametrize(
    "schema, expected",
    [
        ({}, {}),
        ({1: 0}, {}),
        ({1: {"oneOf": 0}}, {}),
        ({1: {"oneOf": []}}, {1: []}),
        ({1: {"oneOf": ["sum"]}}, {}),
        ({1: {"oneOf": [{"$ref": "sum"}]}}, {1: ["sum"]}),
        ({1: {"oneOf": [{"$ref": "sum"}, {"$ref": "sum2"}]}}, {1: ["sum", "sum2"]}),
    ],
)
def test_get_nested_objects(schema, expected):
    assert get_nested_dict(schema) == expected


@pytest.mark.parametrize(
    "schema, expected",
    [
        ({}, {}),
        ({1: 0}, {}),
        ({1: {"oneOf": 0}}, {}),
        ({1: {"oneOf": []}}, {}),
        ({1: {"oneOf": ["sum"]}}, {}),
        ({1: {"properties": {"query_kind": "DUMMY"}}}, {1: {"query_kind": "DUMMY"}}),
        (
            {1: {"properties": {"query_kind": "DUMMY"}}, 2: "NOT_A_QUERY"},
            {1: {"query_kind": "DUMMY"}},
        ),
    ],
)
def test_get_queries(schema, expected):
    assert get_queries(schema) == expected


@pytest.mark.parametrize(
    "query_dict, nested_query_dict, expected",
    [
        ({}, {}, {}),
        (
            {
                "DUMMY": {"query_kind": "DUMMY"},
                "A_DUMMY_CONTAINING_A_DUMMY": {
                    "query_kind": "A_DUMMY_CONTAINING_A_DUMMY",
                    "dummy_param": {"$ref": "MULTI_QUERY"},
                },
            },
            {},
            {"A_DUMMY_CONTAINING_A_DUMMY": {"dummy_param": "MULTI_QUERY"}, "DUMMY": {}},
        ),
        (
            {
                "DUMMY": {"query_kind": "DUMMY"},
                "A_DUMMY_CONTAINING_A_DUMMY": {
                    "query_kind": "A_DUMMY_CONTAINING_A_DUMMY",
                    "dummy_param": {"$ref": "MULTI_QUERY"},
                },
            },
            {"MULTI_QUERY": ["DUMMY"]},
            {"A_DUMMY_CONTAINING_A_DUMMY": {"dummy_param": ["DUMMY"]}, "DUMMY": {}},
        ),
    ],
)
def test_get_reffed_params(query_dict, nested_query_dict, expected):
    assert get_reffed_params(query_dict, nested_query_dict) == expected


@pytest.mark.parametrize(
    "roots, q_tree, expected",
    [
        ([], {}, {}),
        (["DUMMY_ROOT"], {"DUMMY_ROOT": {}}, {"DUMMY_ROOT": {}}),
        (["DUMMY_ROOT"], {"DUMMY_ROOT": {"param": []}}, {"DUMMY_ROOT": {"param": {}}}),
        (
            ["DUMMY_ROOT"],
            {"DUMMY_ROOT": {"param": [1, 2, 3]}},
            {"DUMMY_ROOT": {"param": {1: {}, 2: {}, 3: {}}}},
        ),
        (
            ["DUMMY_ROOT"],
            {"DUMMY_ROOT": {"long_param": [1, 2, 3], "short_param": [1]}},
            {
                "DUMMY_ROOT": {
                    "long_param": {
                        1: {"short_param": {1: {}}},
                        2: {"short_param": {1: {}}},
                        3: {"short_param": {1: {}}},
                    }
                }
            },
        ),
        (
            ["DUMMY_ROOT"],
            {"DUMMY_ROOT": {"long_param": [1, 2, 3], "short_param": [1, 2]}},
            {
                "DUMMY_ROOT": {
                    "long_param": {
                        1: {"short_param": {1: {}, 2: {}}},
                        2: {"short_param": {1: {}, 2: {}}},
                        3: {"short_param": {1: {}, 2: {}}},
                    }
                }
            },
        ),
    ],
)
def test_build_tree(roots, q_tree, expected):
    assert build_tree(roots, q_tree) == expected


@pytest.mark.parametrize(
    "tree, expected",
    [
        ({}, [([], {})]),
        ({1: {}}, [([1], {})]),
        ({1: {2: {}}}, [([1, 2], {})]),
        ({1: {2: {}}, 3: {}}, [([1, 2], {}), ([3], {})]),
    ],
)
def test_enum_paths(tree, expected):
    assert list(enum_paths([], tree)) == expected


@pytest.mark.parametrize(
    "tree, all_queries, expected",
    [
        ({}, {}, []),
        ({"DUMMY": {}}, {"DUMMY": {"query_kind": {"enum": ["dummy"]}}}, ["dummy"]),
        (
            {"DUMMY": {}},
            {
                "DUMMY": {
                    "query_kind": {"enum": ["dummy"]},
                    "aggregation_unit": {"enum": ["DUMMY_UNIT"]},
                }
            },
            ["dummy:aggregation_unit:DUMMY_UNIT",],
        ),
    ],
)
def test_make_per_query_scopes(tree, all_queries, expected):
    assert list(make_per_query_scopes(tree, all_queries)) == expected


@pytest.mark.parametrize(
    "tree, all_queries, expected",
    [
        ({}, {}, ["get_result:available_dates"]),
        (
            {"DUMMY": {}},
            {"DUMMY": {"query_kind": {"enum": ["dummy"]}}},
            ["get_result:dummy", "run:dummy", "get_result:available_dates"],
        ),
        (
            {"DUMMY": {}},
            {
                "DUMMY": {
                    "query_kind": {"enum": ["dummy"]},
                    "aggregation_unit": {"enum": ["DUMMY_UNIT"]},
                }
            },
            [
                "get_result:dummy:aggregation_unit:DUMMY_UNIT",
                "run:dummy:aggregation_unit:DUMMY_UNIT",
                "get_result:available_dates",
            ],
        ),
    ],
)
def test_make_scopes(tree, all_queries, expected):
    assert list(make_scopes(tree, all_queries)) == expected


@pytest.mark.parametrize(
    "schema, expected",
    [
        ({"FlowmachineQuerySchema": {"oneOf": []}}, ["get_result:available_dates"]),
        (
            {
                "FlowmachineQuerySchema": {"oneOf": [{"$ref": "DUMMY"}]},
                "DUMMY": {"properties": {"query_kind": {"enum": ["dummy"]}}},
            },
            ["get_result:dummy", "run:dummy", "get_result:available_dates"],
        ),
        (
            {
                "NESTED_DUMMY": {
                    "properties": {"query_kind": {"enum": ["nested_dummy"]}}
                },
                "NESTED_DUMMY_INPUT": {"oneOf": [{"$ref": "NESTED_DUMMY"}]},
                "FlowmachineQuerySchema": {"oneOf": [{"$ref": "DUMMY"}]},
                "DUMMY": {
                    "properties": {
                        "query_kind": {"enum": ["dummy"]},
                        "dummy_param": {"$ref": "NESTED_DUMMY_INPUT"},
                    }
                },
            },
            [
                "get_result:dummy:dummy_param:nested_dummy",
                "run:dummy:dummy_param:nested_dummy",
                "get_result:available_dates",
            ],
        ),
        (
            {
                "NESTED_DUMMY": {
                    "properties": {
                        "query_kind": {"enum": ["nested_dummy"]},
                        "aggregation_unit": {"enum": ["DUMMY_UNIT"]},
                    }
                },
                "NESTED_DUMMY_INPUT": {"oneOf": [{"$ref": "NESTED_DUMMY"}]},
                "FlowmachineQuerySchema": {"oneOf": [{"$ref": "DUMMY"}]},
                "DUMMY": {
                    "properties": {
                        "query_kind": {"enum": ["dummy"]},
                        "dummy_param": {"$ref": "NESTED_DUMMY_INPUT"},
                    }
                },
            },
            [
                "get_result:dummy:dummy_param:nested_dummy:aggregation_unit:DUMMY_UNIT",
                "run:dummy:dummy_param:nested_dummy:aggregation_unit:DUMMY_UNIT",
                "get_result:available_dates",
            ],
        ),
        (
            {
                "NESTED_DUMMY": {
                    "properties": {
                        "query_kind": {"enum": ["nested_dummy"]},
                        "aggregation_unit": {"enum": ["DUMMY_UNIT"]},
                    }
                },
                "NESTED_DUMMY_2": {
                    "properties": {
                        "query_kind": {"enum": ["nested_dummy_2"]},
                        "aggregation_unit": {"enum": ["DUMMY_UNIT_2"]},
                    }
                },
                "NESTED_DUMMY_INPUT": {
                    "oneOf": [{"$ref": "NESTED_DUMMY"}, {"$ref": "NESTED_DUMMY_2"}]
                },
                "FlowmachineQuerySchema": {"oneOf": [{"$ref": "DUMMY"}]},
                "DUMMY": {
                    "properties": {
                        "query_kind": {"enum": ["dummy"]},
                        "dummy_param": {"$ref": "NESTED_DUMMY_INPUT"},
                        "dummy_param_2": {"$ref": "NESTED_DUMMY_INPUT"},
                    }
                },
            },
            [
                "get_result:dummy:dummy_param:nested_dummy:dummy_param_2:nested_dummy:aggregation_unit:DUMMY_UNIT",
                "get_result:dummy:dummy_param:nested_dummy:dummy_param_2:nested_dummy_2:aggregation_unit:DUMMY_UNIT_2",
                "get_result:dummy:dummy_param:nested_dummy_2:dummy_param_2:nested_dummy:aggregation_unit:DUMMY_UNIT",
                "get_result:dummy:dummy_param:nested_dummy_2:dummy_param_2:nested_dummy_2:aggregation_unit:DUMMY_UNIT_2",
                "run:dummy:dummy_param:nested_dummy:dummy_param_2:nested_dummy:aggregation_unit:DUMMY_UNIT",
                "run:dummy:dummy_param:nested_dummy:dummy_param_2:nested_dummy_2:aggregation_unit:DUMMY_UNIT_2",
                "run:dummy:dummy_param:nested_dummy_2:dummy_param_2:nested_dummy:aggregation_unit:DUMMY_UNIT",
                "run:dummy:dummy_param:nested_dummy_2:dummy_param_2:nested_dummy_2:aggregation_unit:DUMMY_UNIT_2",
                "get_result:available_dates",
            ],
        ),
    ],
)
def test_schema_to_scopes(schema, expected):
    assert list(schema_to_scopes(schema)) == expected
