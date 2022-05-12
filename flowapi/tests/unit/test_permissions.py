# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from flowapi.permissions import (
    per_query_scopes,
    tree_walk_to_scope_list,
    valid_tree_walks,
    is_flat,
    flatten,
    flatten_on_key,
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
        ({}, []),
        (
            {"properties": {"query_kind": {"enum": ["dummy"]}}},
            ["dummy"],
        ),
        (
            {
                "properties": {
                    "query_kind": {"enum": ["dummy"]},
                    "aggregation_unit": {"enum": ["DUMMY_UNIT", "DUMMY_UNIT_2"]},
                }
            },
            [
                "dummy:DUMMY_UNIT",
                "dummy:DUMMY_UNIT_2",
            ],
        ),
        ({"oneOf": []}, []),
        (
            {"oneOf": [{"properties": {"query_kind": {"enum": ["dummy"]}}}]},
            ["dummy"],
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
                "dummy:dummy_param",
                "nested_dummy",
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
                "dummy",
                "nested_dummy:DUMMY_UNIT",
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
                "dummy",
                "nested_dummy:DUMMY_UNIT",
                "nested_dummy:DUMMY_UNIT_2",
                "nested_dummy_2:DUMMY_UNIT_2",
                "nested_dummy_2:DUMMY_UNIT_3",
            ],
        ),
    ],
)
def test_per_query_scopes(tree, expected):
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


@pytest.mark.parametrize(
    "input, expected",
    [
        ({"flat": "dict"}, True),
        ({"flat": "dict", "multi": "keys"}, True),
        ({"outer": {"inner": "dict"}}, False),
        ({"outer": ["inner", "list"]}, False),
        (["flat", "list"], True),
        (["nested", {"inner": "dict"}], False),
        (["nested", ["list"]], False),
        ({"outer": {"middle": {"inner": "dict"}}}, False),
    ],
)
def test_is_flat(input, expected):
    assert is_flat(input) == expected


@pytest.mark.parametrize(
    "input, expected",
    [
        ({"flat": "dict"}, [{"flat": "dict"}]),
        (["flat", "list"], ["flat", "list"]),
        (
            {"outer1": {"inner": "1"}, "outer2": {"inner": "2"}},
            [{"inner": "1"}, {"inner": "2"}],
        ),
        (
            {"outer_0": {"flat": "1"}, "outer_2": {"middle": {"flat": "2"}}},
            [{"flat": "1"}, {"flat": "2"}],
        ),
    ],
)
def test_flatten(input, expected):
    assert (flatten(input)) == expected


@pytest.mark.parametrize(
    "input, expected",
    [
        (
            {"oneOf": [{"properties": {"query_kind": {"enum": ["dummy"]}}}]},
            [{"query_kind": {"enum": ["dummy"]}}],
        ),
        (
            # input
            {
                "oneOf": [
                    {
                        "properties": {
                            "query_kind": {"enum": ["dummy"]},
                            "dummy_param_1": {
                                "properties": {
                                    "query_kind": {"enum": ["nested_dummy"]},
                                    "aggregation_unit": {"enum": ["DUMMY_UNIT"]},
                                }
                            },
                            "dummy_param_2": {
                                "properties": {
                                    "query_kind": {"enum": ["nested_dummy_2"]},
                                    "aggregation_unit": {"enum": ["DUMMY_UNIT_2"]},
                                }
                            },
                        }
                    }
                ]
            },
            # expected
            [
                {
                    "query_kind": {"enum": ["nested_dummy"]},
                    "aggregation_unit": {"enum": ["DUMMY_UNIT"]},
                },
                {
                    "query_kind": {"enum": ["nested_dummy_2"]},
                    "aggregation_unit": {"enum": ["DUMMY_UNIT_2"]},
                },
                {"query_kind": {"enum": ["dummy"]}},
            ],
        ),
    ],
)
def test_flatten_on_key(input, expected):
    assert flatten_on_key(input, key="properties") == expected
