# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from unittest.mock import Mock

import pytest

pytest_plugins = ["pytester"]


@pytest.fixture
def dummy_flowapi(monkeypatch):
    """
    Fixture which monkeypatches requests.get to return a dummy flowapi schema and returns
    the expected decoded user claims dict.
    """
    get_mock = Mock()
    get_mock.return_value.json.return_value = {
        "components": {
            "schemas": {
                "DUMMY_QUERY": {
                    "properties": {
                        "query_kind": {"enum": ["DUMMY_QUERY_KIND"]},
                        "aggregation_unit": {
                            "enum": ["admin0", "admin1", "admin2", "admin3"],
                            "type": "string",
                        },
                    }
                },
                "NOT_A_DUMMY_QUERY": {
                    "properties": {
                        "aggregation_unit": {
                            "enum": ["admin0", "admin1", "admin2", "admin3"],
                            "type": "string",
                        }
                    }
                },
                "DUMMY_QUERY_WITH_NO_AGGREGATIONS": {
                    "properties": {
                        "query_kind": {
                            "enum": ["DUMMY_QUERY_WITH_NO_AGGREGATIONS_KIND"]
                        }
                    }
                },
            }
        }
    }
    monkeypatch.setattr("requests.get", get_mock)
    return {
        "DUMMY_QUERY_KIND": {
            "permissions": {"get_result": True, "poll": True, "run": True},
            "spatial_aggregation": ["admin0", "admin1", "admin2", "admin3"],
        },
        "DUMMY_QUERY_WITH_NO_AGGREGATIONS_KIND": {
            "permissions": {"get_result": True, "poll": True, "run": True},
            "spatial_aggregation": ["admin0", "admin1", "admin2", "admin3"],
        },
        "available_dates": {"permissions": {"get_result": True}},
        "geography": {
            "permissions": {"get_result": True, "poll": True, "run": True},
            "spatial_aggregation": ["admin0", "admin1", "admin2", "admin3"],
        },
    }
