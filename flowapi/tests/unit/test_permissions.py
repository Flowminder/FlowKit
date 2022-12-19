# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import prance
import quart
from flowapi.permissions import (
    is_flat,
    flatten_on_key,
    tl_schema_scope_string,
    schema_to_scopes,
    grab_on_key_list,
    get_agg_unit,
)

import pytest
import asyncio
import ast

pytest_plugins = "pytest_asyncio"


@pytest.mark.parametrize(
    "tree, expected",
    [
        (
            {"oneOf": [{"properties": {"query_kind": {"enum": ["dummy"]}}}]},
            ["nonspatial:dummy:dummy"],
        ),
        (
            {
                "oneOf": [
                    {
                        "properties": {
                            "query_kind": {"enum": ["dummy"]},
                            "aggregation_unit": {"enum": ["DUMMY_UNIT"]},
                            "dummy_param": {
                                "properties": {"query_kind": {"enum": ["nested_dummy"]}}
                            },
                        }
                    }
                ]
            },
            [
                "DUMMY_UNIT:dummy:dummy",
                "DUMMY_UNIT:dummy:nested_dummy",
            ],
        ),
        (
            {
                "oneOf": [
                    {
                        "properties": {
                            "query_kind": {"enum": ["dummy"]},
                            "aggregation_unit": {"enum": ["TL_DUMMY_UNIT"]},
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
                "TL_DUMMY_UNIT:dummy:dummy",
                "TL_DUMMY_UNIT:dummy:nested_dummy",
            ],
        ),
        (
            {
                "oneOf": [
                    {
                        "properties": {
                            "query_kind": {"enum": ["dummy"]},
                            "enum": ["TL_DUMMY_UNIT", "TL_DUMMY_UNIT_2"],
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
                "nonspatial:dummy:dummy",
                "nonspatial:dummy:nested_dummy",
                "nonspatial:dummy:nested_dummy_2",
            ],
        ),
    ],
)
def test_schema_to_scopes(tree, expected, monkeypatch):
    # Shouldn't try and fit a full spec in here, this test is large enough as it is - we skip ResolvingParser instead
    class MockResolvingParser:
        def __init__(self, spec_string, **kwargs):
            self.specification = {
                "components": {
                    "schemas": {"FlowmachineQuerySchema": ast.literal_eval(spec_string)}
                }
            }

    # It looks like we can't mock out ResolvingParser, so we mock out it's parent instead
    monkeypatch.setattr(prance, "BaseParser", MockResolvingParser)

    class MockFlowApiLogger:
        @staticmethod
        def warning(msg):
            print(msg)

    class MockCurrentApp:
        flowapi_logger = MockFlowApiLogger()

    monkeypatch.setattr(quart, "current_app", MockCurrentApp)
    assert schema_to_scopes(tree) == expected


def test_schema_to_scopes_bad_input():
    with pytest.raises(
        AssertionError, match="No specification parsed, cannot validate!"
    ):
        schema_to_scopes({})


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


def test_scopes_from_query():
    tl_query = {
        "properties": {
            "query_kind": {"enum": ["test_query"]},
            "aggregation_unit": {"enum": ["DUMMY_UNIT", "DUMMY_UNIT_2"]},
        }
    }

    input = "nested_query"
    expected = {
        "DUMMY_UNIT:test_query:nested_query",
        "DUMMY_UNIT_2:test_query:nested_query",
    }
    assert tl_schema_scope_string(tl_query, input) == expected


def test_grab_on_key_list():

    input = {"1": [{}, {}, {"3": "success"}]}
    keys = ["1", 2, "3"]
    assert grab_on_key_list(input, keys) == ["success"]

    input = {"outer": {"not_inner": "wrong", "inner": "right"}}
    keys = ["outer", "inner"]
    assert grab_on_key_list(input, keys) == ["right"]

    input = {
        "first": {"1": {"2": "first_inner"}},
        "second": {"1": {"2": "second_inner"}},
        "third": {"1": {"3": "not_needed"}},
    }
    keys = ["1", "2"]
    assert grab_on_key_list(input, keys) == ["first_inner", "second_inner"]

    input = {}
