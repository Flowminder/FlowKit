# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import pytest

from flowapi.permissions import (
    make_per_query_scopes,
    make_scopes,
    walk_tree,
)


@pytest.mark.parametrize(
    "tree, expected",
    [
        ({}, [([], {})]),
        ({1: {}}, [([1], {})]),
        ({1: {2: {}}}, [([1, 2], {})]),
        ({1: {2: {}}, 3: {}}, [([1, 2], {}), ([3], {})]),
    ],
)
def test_walk_tree(tree, expected):
    assert list(walk_tree(tree=tree)) == expected


@pytest.mark.parametrize(
    "tree, expected",
    [
        ({}, []),
        ({"properties": {"query_kind": {"enum": ["dummy"]}}}, ["dummy"]),
        (
            {
                "properties": {
                    "query_kind": {"enum": ["dummy"]},
                    "aggregation_unit": {"enum": ["DUMMY_UNIT", "DUMMY_UNIT_2"]},
                }
            },
            [
                "dummy:aggregation_unit:DUMMY_UNIT",
                "dummy:aggregation_unit:DUMMY_UNIT_2",
            ],
        ),
    ],
)
def test_make_per_query_scopes(tree, expected):
    assert list(make_per_query_scopes(queries=tree)) == expected


@pytest.mark.parametrize(
    "tree, expected",
    [
        ({}, ["get_result:available_dates"]),
        (
            {"properties": {"query_kind": {"enum": ["dummy"]}}},
            ["get_result:dummy", "run:dummy", "get_result:available_dates"],
        ),
        (
            {
                "properties": {
                    "query_kind": {"enum": ["dummy"]},
                    "aggregation_unit": {"enum": ["DUMMY_UNIT", "DUMMY_UNIT_2"]},
                }
            },
            [
                "get_result:dummy:aggregation_unit:DUMMY_UNIT",
                "get_result:dummy:aggregation_unit:DUMMY_UNIT_2",
                "run:dummy:aggregation_unit:DUMMY_UNIT",
                "run:dummy:aggregation_unit:DUMMY_UNIT_2",
                "get_result:available_dates",
            ],
        ),
        (
            {
                "properties": {
                    "query_kind": {"enum": ["dummy"]},
                    "dummy_param": {
                        "oneOf": [
                            {
                                "properties": {
                                    "query_kind": {"enum": ["nested_dummy_a"]}
                                }
                            },
                            {
                                "properties": {
                                    "query_kind": {"enum": ["nested_dummy_b"]}
                                }
                            },
                        ]
                    },
                    "dummy_param_2": {
                        "properties": {
                            "query_kind": {"enum": ["nested_dummy_2"]},
                            "aggregation_unit": {
                                "enum": ["DUMMY_UNIT_2", "DUMMY_UNIT_3"]
                            },
                        },
                    },
                },
            },
            [
                "get_result:dummy:dummy_param:nested_dummy_a:dummy_param_2:nested_dummy_2:aggregation_unit:DUMMY_UNIT_2",
                "get_result:dummy:dummy_param:nested_dummy_a:dummy_param_2:nested_dummy_2:aggregation_unit:DUMMY_UNIT_3",
                "get_result:dummy:dummy_param:nested_dummy_b:dummy_param_2:nested_dummy_2:aggregation_unit:DUMMY_UNIT_2",
                "get_result:dummy:dummy_param:nested_dummy_b:dummy_param_2:nested_dummy_2:aggregation_unit:DUMMY_UNIT_3",
                "run:dummy:dummy_param:nested_dummy_a:dummy_param_2:nested_dummy_2:aggregation_unit:DUMMY_UNIT_2",
                "run:dummy:dummy_param:nested_dummy_a:dummy_param_2:nested_dummy_2:aggregation_unit:DUMMY_UNIT_3",
                "run:dummy:dummy_param:nested_dummy_b:dummy_param_2:nested_dummy_2:aggregation_unit:DUMMY_UNIT_2",
                "run:dummy:dummy_param:nested_dummy_b:dummy_param_2:nested_dummy_2:aggregation_unit:DUMMY_UNIT_3",
                "get_result:available_dates",
            ],
        ),
    ],
)
def test_make_scopes(tree, expected):
    assert list(make_scopes(queries=tree)) == expected


@pytest.mark.parametrize(
    "schema, expected",
    [
        ({"oneOf": []}, ["get_result:available_dates"]),
        (
            {"oneOf": [{"properties": {"query_kind": {"enum": ["dummy"]}}}]},
            ["get_result:dummy", "run:dummy", "get_result:available_dates"],
        ),
        (
            {
                "oneOf": [
                    {
                        "properties": {
                            "query_kind": {"enum": ["dummy"]},
                            "dummy_param": {
                                "properties": {"query_kind": {"enum": ["nested_dummy"]}}
                            },
                        }
                    }
                ]
            },
            [
                "get_result:dummy:dummy_param:nested_dummy",
                "run:dummy:dummy_param:nested_dummy",
                "get_result:available_dates",
            ],
        ),
        (
            {
                "oneOf": [
                    {
                        "properties": {
                            "query_kind": {"enum": ["dummy"]},
                            "dummy_param": {
                                "properties": {
                                    "query_kind": {"enum": ["nested_dummy"]},
                                    "aggregation_unit": {"enum": ["DUMMY_UNIT"]},
                                }
                            },
                        }
                    }
                ]
            },
            [
                "get_result:dummy:dummy_param:nested_dummy",
                "run:dummy:dummy_param:nested_dummy",
                "get_result:available_dates",
            ],
        ),
        (
            {
                "oneOf": [
                    {
                        "properties": {
                            "query_kind": {"enum": ["dummy"]},
                            "dummy_param": {
                                "properties": {
                                    "query_kind": {"enum": ["nested_dummy"]},
                                    "aggregation_unit": {
                                        "enum": ["DUMMY_UNIT", "DUMMY_UNIT_2"]
                                    },
                                }
                            },
                            "dummy_param_2": {
                                "properties": {
                                    "query_kind": {"enum": ["nested_dummy_2"]},
                                    "aggregation_unit": {
                                        "enum": ["DUMMY_UNIT_2", "DUMMY_UNIT_3"]
                                    },
                                },
                            },
                        },
                    }
                ]
            },
            [
                "get_result:dummy:dummy_param:nested_dummy:aggregation_unit:DUMMY_UNIT:dummy_param_2:nested_dummy_2:aggregation_unit:DUMMY_UNIT_2",
                "get_result:dummy:dummy_param:nested_dummy:aggregation_unit:DUMMY_UNIT:dummy_param_2:nested_dummy_2:aggregation_unit:DUMMY_UNIT_3",
                "get_result:dummy:dummy_param:nested_dummy:aggregation_unit:DUMMY_UNIT_2:dummy_param_2:nested_dummy_2:aggregation_unit:DUMMY_UNIT_2",
                "get_result:dummy:dummy_param:nested_dummy:aggregation_unit:DUMMY_UNIT_2:dummy_param_2:nested_dummy_2:aggregation_unit:DUMMY_UNIT_3",
                "run:dummy:dummy_param:nested_dummy:aggregation_unit:DUMMY_UNIT:dummy_param_2:nested_dummy_2:aggregation_unit:DUMMY_UNIT_2",
                "run:dummy:dummy_param:nested_dummy:aggregation_unit:DUMMY_UNIT:dummy_param_2:nested_dummy_2:aggregation_unit:DUMMY_UNIT_3",
                "run:dummy:dummy_param:nested_dummy:aggregation_unit:DUMMY_UNIT_2:dummy_param_2:nested_dummy_2:aggregation_unit:DUMMY_UNIT_2",
                "run:dummy:dummy_param:nested_dummy:aggregation_unit:DUMMY_UNIT_2:dummy_param_2:nested_dummy_2:aggregation_unit:DUMMY_UNIT_3",
                "get_result:available_dates",
            ],
        ),
    ],
)
def test_schema_to_scopes(schema, expected):
    assert list(make_scopes(queries=schema)) == expected
