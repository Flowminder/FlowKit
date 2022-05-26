# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from flowapi.permissions import (
    is_flat,
    flatten_on_key,
    scopes_from_query,
    schema_to_scopes,
)

import pytest


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
                "dummy",
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
                "dummy",
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
                "nested_dummy",
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
                "nested_dummy",
                "nested_dummy:DUMMY_UNIT",
                "nested_dummy:DUMMY_UNIT_2",
                "nested_dummy_2",
                "nested_dummy_2:DUMMY_UNIT_2",
                "nested_dummy_2:DUMMY_UNIT_3",
            ],
        ),
        ({"not_a_query": "empty"}, []),
    ],
)
def test_schema_to_scopes(tree, expected):
    assert schema_to_scopes(tree) == expected


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
        ({}, True),
        ({"none_type": None, "bool_type": True}, True),
        ({"none_type": None, "nested": {"inner": "dict"}}, False),
        (None, True),
        (True, True),
    ],
)
def test_is_flat(input, expected):
    assert is_flat(input) == expected


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
        (
            # input
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
            # expected
            [
                {
                    "query_kind": {"enum": ["nested_dummy"]},
                    "aggregation_unit": {"enum": ["DUMMY_UNIT"]},
                },
                {"query_kind": {"enum": ["dummy"]}},
            ],
        ),
    ],
)
def test_flatten_on_key(input, expected):
    assert flatten_on_key(input, key="properties") == expected


@pytest.mark.parametrize(
    "input,expected",
    [
        (
            {
                "query_kind": {"enum": ["nested_dummy"]},
                "aggregation_unit": {"enum": ["DUMMY_UNIT", "DUMMY_UNIT_2"]},
            },
            {"nested_dummy", "nested_dummy:DUMMY_UNIT", "nested_dummy:DUMMY_UNIT_2"},
        ),
        ({"query_kind": {"enum": ["dummy"]}}, {"dummy"}),
    ],
)
def test_scopes_from_query(input, expected):
    assert scopes_from_query(input) == expected
