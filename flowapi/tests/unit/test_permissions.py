# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from flowapi.permissions import (
    per_query_scopes,
    tree_walk_to_scope_list,
    valid_tree_walks,
)

import pytest


@pytest.mark.parametrize(
    "tree, expected",
    [
        ({}, []),
        ({1: {}}, [[1]]),
        ({1: {2: {}}}, [[1, 2]]),
        ({1: {2: {}}, 3: {}}, [[1, 2], [3]]),
        (
            {1: {2: [1, 2, 3, 4]}, "3": "A"},
            [[1, 2, 1], [1, 2, 2], [1, 2, 3], [1, 2, 4], ["3", "A"]],
        ),
    ],
)
def test_valid_tree_walks(tree, expected):
    assert list(valid_tree_walks(tree)) == expected


@pytest.mark.parametrize(
    "tree, expected",
    [
        ({}, ["get_result&available_dates"]),
        (
            {"properties": {"query_kind": {"enum": ["dummy"]}}},
            ["get_result&dummy", "run&dummy", "get_result&available_dates"],
        ),
        (
            {
                "properties": {
                    "query_kind": {"enum": ["dummy"]},
                    "aggregation_unit": {"enum": ["DUMMY_UNIT", "DUMMY_UNIT_2"]},
                }
            },
            [
                "get_result&dummy.aggregation_unit.DUMMY_UNIT",
                "run&dummy.aggregation_unit.DUMMY_UNIT",
                "get_result&dummy.aggregation_unit.DUMMY_UNIT_2",
                "run&dummy.aggregation_unit.DUMMY_UNIT_2",
                "get_result&available_dates",
            ],
        ),
        ({"oneOf": []}, ["get_result&available_dates"]),
        (
            {"oneOf": [{"properties": {"query_kind": {"enum": ["dummy"]}}}]},
            ["get_result&dummy", "run&dummy", "get_result&available_dates"],
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
                "get_result&dummy.dummy_param.nested_dummy",
                "run&dummy.dummy_param.nested_dummy",
                "get_result&available_dates",
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
                "get_result&dummy.dummy_param.nested_dummy",
                "run&dummy.dummy_param.nested_dummy",
                "get_result&available_dates",
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
                "get_result&dummy.dummy_param.nested_dummy.aggregation_unit.DUMMY_UNIT&dummy.dummy_param_2.nested_dummy_2.aggregation_unit.DUMMY_UNIT_2",
                "run&dummy.dummy_param.nested_dummy.aggregation_unit.DUMMY_UNIT&dummy.dummy_param_2.nested_dummy_2.aggregation_unit.DUMMY_UNIT_2",
                "get_result&dummy.dummy_param.nested_dummy.aggregation_unit.DUMMY_UNIT&dummy.dummy_param_2.nested_dummy_2.aggregation_unit.DUMMY_UNIT_3",
                "run&dummy.dummy_param.nested_dummy.aggregation_unit.DUMMY_UNIT&dummy.dummy_param_2.nested_dummy_2.aggregation_unit.DUMMY_UNIT_3",
                "get_result&dummy.dummy_param.nested_dummy.aggregation_unit.DUMMY_UNIT_2&dummy.dummy_param_2.nested_dummy_2.aggregation_unit.DUMMY_UNIT_2",
                "run&dummy.dummy_param.nested_dummy.aggregation_unit.DUMMY_UNIT_2&dummy.dummy_param_2.nested_dummy_2.aggregation_unit.DUMMY_UNIT_2",
                "get_result&dummy.dummy_param.nested_dummy.aggregation_unit.DUMMY_UNIT_2&dummy.dummy_param_2.nested_dummy_2.aggregation_unit.DUMMY_UNIT_3",
                "run&dummy.dummy_param.nested_dummy.aggregation_unit.DUMMY_UNIT_2&dummy.dummy_param_2.nested_dummy_2.aggregation_unit.DUMMY_UNIT_3",
                "get_result&available_dates",
            ],
        ),
    ],
)
def test_per_query_scopes(tree, expected):

    if tree == {
        "properties": {
            "query_kind": {"enum": ["dummy"]},
            "aggregation_unit": {"enum": ["DUMMY_UNIT", "DUMMY_UNIT_2"]},
        }
    } or tree == {
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
    }:
        pytest.xfail(
            "Under new schema rules, cannot presently mix admin levels in the same query. See bug #4649"
        )

    assert list(per_query_scopes(queries=tree)) == expected


@pytest.mark.parametrize(
    "walk, expected",
    [
        ("DUMMY", ["DUMMY"]),
        (["DUMMY", "DUMMY"], ["DUMMY.DUMMY"]),
        ((["DUMMY", "DUMMY"], ["DUMMY", "DUMMY"]), ["DUMMY.DUMMY", "DUMMY.DUMMY"]),
        ((["DUMMY", "DUMMY"], (["DUMMY", "DUMMY"])), ["DUMMY.DUMMY", "DUMMY.DUMMY"]),
    ],
)
def test_tree_walk_to_scope_list(walk, expected):
    assert list(tree_walk_to_scope_list(walk)) == expected
