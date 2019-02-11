# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest

from flowmachine.core.server.query_proxy import (
    construct_query_object,
    QueryProxyError,
    InvalidGeographyError,
)
from flowmachine.core import GeoTable
from flowmachine.features import LastLocation, ModalLocation, Flows, TotalLocationEvents


@pytest.mark.parametrize(
    "expected_md5, query_spec",
    [
        (
            "010f0baa76e0724215613b8486a71a49",
            {
                "query_kind": "daily_location",
                "params": {
                    "date": "2016-01-01",
                    "aggregation_unit": "admin3",
                    "daily_location_method": "last",
                    "subscriber_subset": "all",
                },
            },
        ),
        (
            "bf3dba82e8a7e404fda6d66f8a900fb9",
            {
                "query_kind": "location_event_counts",
                "params": {
                    "start_date": "2016-01-01",
                    "end_date": "2016-01-02",
                    "interval": "day",
                    "aggregation_unit": "admin3",
                    "direction": "all",
                    "event_types": "all",
                    "subscriber_subset": "all",
                },
            },
        ),
        (
            "21e89d6cc510cd333c199ab8c50ad0c0",
            {
                "query_kind": "modal_location",
                "params": {
                    "locations": (
                        {
                            "query_kind": "daily_location",
                            "params": {
                                "date": "2016-01-01",
                                "aggregation_unit": "admin3",
                                "daily_location_method": "last",
                                "subscriber_subset": "all",
                            },
                        },
                        {
                            "query_kind": "daily_location",
                            "params": {
                                "date": "2016-01-02",
                                "aggregation_unit": "admin3",
                                "daily_location_method": "last",
                                "subscriber_subset": "all",
                            },
                        },
                    ),
                    "aggregation_unit": "admin3",
                },
            },
        ),
        (
            "5b2b2484e941da429b52dda7e81fb917",
            {"query_kind": "geography", "params": {"aggregation_unit": "admin3"}},
        ),
        (
            "bdd98c3db9128a1e7ec3e9d64ff57ff7",
            {
                "query_kind": "meaningful_locations_aggregate",
                "params": {
                    "aggregation_unit": "admin1",
                    "meaningful_locations": {
                        "query_kind": "meaningful_locations",
                        "params": {
                            "label": "unknown",
                            "clusters": {
                                "query_kind": "hartigan_cluster",
                                "params": {
                                    "radius": 1.0,
                                    "buffer": 0.0,
                                    "call_threshold": 0,
                                    "call_days": {
                                        "query_kind": "call_days",
                                        "params": {
                                            "subscriber_locations": {
                                                "query_kind": "subscriber_locations",
                                                "params": {
                                                    "start": "2016-01-01",
                                                    "stop": "2016-01-02",
                                                    "level": "versioned-site",
                                                    "subscriber_subset": "all",
                                                },
                                            }
                                        },
                                    },
                                },
                            },
                            "scores": {
                                "query_kind": "event_score",
                                "params": {
                                    "score_hour": [
                                        -1,
                                        -1,
                                        -1,
                                        -1,
                                        -1,
                                        -1,
                                        -1,
                                        0,
                                        0,
                                        1,
                                        1,
                                        1,
                                        1,
                                        1,
                                        1,
                                        1,
                                        1,
                                        0,
                                        0,
                                        0,
                                        0,
                                        -1,
                                        -1,
                                        -1,
                                    ],
                                    "score_dow": {
                                        "monday": 1,
                                        "tuesday": 1,
                                        "wednesday": 1,
                                        "thursday": 0,
                                        "friday": -1,
                                        "saturday": -1,
                                        "sunday": -1,
                                    },
                                    "start": "2016-01-01",
                                    "stop": "2016-01-02",
                                    "level": "versioned-site",
                                    "subscriber_subset": "all",
                                },
                            },
                            "labels": {
                                "evening": {
                                    "type": "Polygon",
                                    "coordinates": [
                                        [
                                            [1e-06, -0.5],
                                            [1e-06, -1.1],
                                            [1.1, -1.1],
                                            [1.1, -0.5],
                                        ]
                                    ],
                                },
                                "day": {
                                    "type": "Polygon",
                                    "coordinates": [
                                        [
                                            [-1.1, -0.5],
                                            [-1.1, 0.5],
                                            [-1e-06, 0.5],
                                            [0, -0.5],
                                        ]
                                    ],
                                },
                            },
                        },
                    },
                },
            },
        ),
        (
            "ad691ab8d2f6f5e1009c08144f74a5ff",
            {
                "query_kind": "meaningful_locations_od_matrix",
                "params": {
                    "aggregation_unit": "admin1",
                    "meaningful_locations_a": {
                        "query_kind": "meaningful_locations",
                        "params": {
                            "label": "unknown",
                            "clusters": {
                                "query_kind": "hartigan_cluster",
                                "params": {
                                    "radius": 1.0,
                                    "buffer": 0.0,
                                    "call_threshold": 0,
                                    "call_days": {
                                        "query_kind": "call_days",
                                        "params": {
                                            "subscriber_locations": {
                                                "query_kind": "subscriber_locations",
                                                "params": {
                                                    "start": "2016-01-01",
                                                    "stop": "2016-01-02",
                                                    "level": "versioned-site",
                                                    "subscriber_subset": "all",
                                                },
                                            }
                                        },
                                    },
                                },
                            },
                            "scores": {
                                "query_kind": "event_score",
                                "params": {
                                    "score_hour": [
                                        -1,
                                        -1,
                                        -1,
                                        -1,
                                        -1,
                                        -1,
                                        -1,
                                        0,
                                        0,
                                        1,
                                        1,
                                        1,
                                        1,
                                        1,
                                        1,
                                        1,
                                        1,
                                        0,
                                        0,
                                        0,
                                        0,
                                        -1,
                                        -1,
                                        -1,
                                    ],
                                    "score_dow": {
                                        "monday": 1,
                                        "tuesday": 1,
                                        "wednesday": 1,
                                        "thursday": 0,
                                        "friday": -1,
                                        "saturday": -1,
                                        "sunday": -1,
                                    },
                                    "start": "2016-01-01",
                                    "stop": "2016-01-02",
                                    "level": "versioned-site",
                                    "subscriber_subset": "all",
                                },
                            },
                            "labels": {
                                "evening": {
                                    "type": "Polygon",
                                    "coordinates": [
                                        [
                                            [1e-06, -0.5],
                                            [1e-06, -1.1],
                                            [1.1, -1.1],
                                            [1.1, -0.5],
                                        ]
                                    ],
                                },
                                "day": {
                                    "type": "Polygon",
                                    "coordinates": [
                                        [
                                            [-1.1, -0.5],
                                            [-1.1, 0.5],
                                            [-1e-06, 0.5],
                                            [0, -0.5],
                                        ]
                                    ],
                                },
                            },
                        },
                    },
                    "meaningful_locations_b": {
                        "query_kind": "meaningful_locations",
                        "params": {
                            "label": "evening",
                            "clusters": {
                                "query_kind": "hartigan_cluster",
                                "params": {
                                    "radius": 1.0,
                                    "buffer": 0.0,
                                    "call_threshold": 0,
                                    "call_days": {
                                        "query_kind": "call_days",
                                        "params": {
                                            "subscriber_locations": {
                                                "query_kind": "subscriber_locations",
                                                "params": {
                                                    "start": "2016-01-01",
                                                    "stop": "2016-01-02",
                                                    "level": "versioned-site",
                                                    "subscriber_subset": "all",
                                                },
                                            }
                                        },
                                    },
                                },
                            },
                            "scores": {
                                "query_kind": "event_score",
                                "params": {
                                    "score_hour": [
                                        -1,
                                        -1,
                                        -1,
                                        -1,
                                        -1,
                                        -1,
                                        -1,
                                        0,
                                        0,
                                        1,
                                        1,
                                        1,
                                        1,
                                        1,
                                        1,
                                        1,
                                        1,
                                        0,
                                        0,
                                        0,
                                        0,
                                        -1,
                                        -1,
                                        -1,
                                    ],
                                    "score_dow": {
                                        "monday": 1,
                                        "tuesday": 1,
                                        "wednesday": 1,
                                        "thursday": 0,
                                        "friday": -1,
                                        "saturday": -1,
                                        "sunday": -1,
                                    },
                                    "start": "2016-01-01",
                                    "stop": "2016-01-02",
                                    "level": "versioned-site",
                                    "subscriber_subset": "all",
                                },
                            },
                            "labels": {
                                "evening": {
                                    "type": "Polygon",
                                    "coordinates": [
                                        [
                                            [1e-06, -0.5],
                                            [1e-06, -1.1],
                                            [1.1, -1.1],
                                            [1.1, -0.5],
                                        ]
                                    ],
                                },
                                "day": {
                                    "type": "Polygon",
                                    "coordinates": [
                                        [
                                            [-1.1, -0.5],
                                            [-1.1, 0.5],
                                            [-1e-06, 0.5],
                                            [0, -0.5],
                                        ]
                                    ],
                                },
                            },
                        },
                    },
                },
            },
        ),
        (
            "ac9acfdf869e20d507ea6ff419acd991",
            {
                "query_kind": "meaningful_locations_od_matrix",
                "params": {
                    "aggregation_unit": "admin1",
                    "meaningful_locations_a": {
                        "query_kind": "meaningful_locations",
                        "params": {
                            "label": "unknown",
                            "clusters": {
                                "query_kind": "hartigan_cluster",
                                "params": {
                                    "radius": 1.0,
                                    "buffer": 0.0,
                                    "call_threshold": 2,
                                    "call_days": {
                                        "query_kind": "call_days",
                                        "params": {
                                            "subscriber_locations": {
                                                "query_kind": "subscriber_locations",
                                                "params": {
                                                    "start": "2016-01-01",
                                                    "stop": "2016-01-02",
                                                    "level": "versioned-site",
                                                    "subscriber_subset": "all",
                                                },
                                            }
                                        },
                                    },
                                },
                            },
                            "scores": {
                                "query_kind": "event_score",
                                "params": {
                                    "score_hour": [
                                        -1,
                                        -1,
                                        -1,
                                        -1,
                                        -1,
                                        -1,
                                        -1,
                                        0,
                                        0,
                                        1,
                                        1,
                                        1,
                                        1,
                                        1,
                                        1,
                                        1,
                                        1,
                                        0,
                                        0,
                                        0,
                                        0,
                                        -1,
                                        -1,
                                        -1,
                                    ],
                                    "score_dow": {
                                        "monday": 1,
                                        "tuesday": 1,
                                        "wednesday": 1,
                                        "thursday": 0,
                                        "friday": -1,
                                        "saturday": -1,
                                        "sunday": -1,
                                    },
                                    "start": "2016-01-01",
                                    "stop": "2016-01-02",
                                    "level": "versioned-site",
                                    "subscriber_subset": "all",
                                },
                            },
                            "labels": {
                                "evening": {
                                    "type": "Polygon",
                                    "coordinates": [
                                        [
                                            [1e-06, -0.5],
                                            [1e-06, -1.1],
                                            [1.1, -1.1],
                                            [1.1, -0.5],
                                        ]
                                    ],
                                },
                                "day": {
                                    "type": "Polygon",
                                    "coordinates": [
                                        [
                                            [-1.1, -0.5],
                                            [-1.1, 0.5],
                                            [-1e-06, 0.5],
                                            [0, -0.5],
                                        ]
                                    ],
                                },
                            },
                        },
                    },
                    "meaningful_locations_b": {
                        "query_kind": "meaningful_locations",
                        "params": {
                            "label": "unknown",
                            "clusters": {
                                "query_kind": "hartigan_cluster",
                                "params": {
                                    "radius": 1.0,
                                    "buffer": 0.0,
                                    "call_threshold": 2,
                                    "call_days": {
                                        "query_kind": "call_days",
                                        "params": {
                                            "subscriber_locations": {
                                                "query_kind": "subscriber_locations",
                                                "params": {
                                                    "start": "2016-01-01",
                                                    "stop": "2016-01-05",
                                                    "level": "versioned-site",
                                                    "subscriber_subset": "all",
                                                },
                                            }
                                        },
                                    },
                                },
                            },
                            "scores": {
                                "query_kind": "event_score",
                                "params": {
                                    "score_hour": [
                                        -1,
                                        -1,
                                        -1,
                                        -1,
                                        -1,
                                        -1,
                                        -1,
                                        0,
                                        0,
                                        1,
                                        1,
                                        1,
                                        1,
                                        1,
                                        1,
                                        1,
                                        1,
                                        0,
                                        0,
                                        0,
                                        0,
                                        -1,
                                        -1,
                                        -1,
                                    ],
                                    "score_dow": {
                                        "monday": 1,
                                        "tuesday": 1,
                                        "wednesday": 1,
                                        "thursday": 0,
                                        "friday": -1,
                                        "saturday": -1,
                                        "sunday": -1,
                                    },
                                    "start": "2016-01-01",
                                    "stop": "2016-01-05",
                                    "level": "versioned-site",
                                    "subscriber_subset": "all",
                                },
                            },
                            "labels": {
                                "evening": {
                                    "type": "Polygon",
                                    "coordinates": [
                                        [
                                            [1e-06, -0.5],
                                            [1e-06, -1.1],
                                            [1.1, -1.1],
                                            [1.1, -0.5],
                                        ]
                                    ],
                                },
                                "day": {
                                    "type": "Polygon",
                                    "coordinates": [
                                        [
                                            [-1.1, -0.5],
                                            [-1.1, 0.5],
                                            [-1e-06, 0.5],
                                            [0, -0.5],
                                        ]
                                    ],
                                },
                            },
                        },
                    },
                },
            },
        ),
    ],
)
def test_construct_query(expected_md5, query_spec):
    """
    Test that expected query objects are constructed by construct_query_object
    """
    obj = construct_query_object(**query_spec)
    assert expected_md5 == obj.md5


def test_wrong_geography_aggregation_unit_raises_error():
    """
    Test that an invalid aggregation unit in a geography query raises an InvalidGeographyError
    """
    with pytest.raises(
        InvalidGeographyError,
        match="Unrecognised aggregation unit 'DUMMY_AGGREGATION_UNIT'",
    ):
        _ = construct_query_object(
            "geography", {"aggregation_unit": "DUMMY_AGGREGATION_UNIT"}
        )


@pytest.mark.parametrize(
    "query_class, query_spec",
    [
        (
            LastLocation,
            {
                "query_kind": "daily_location",
                "params": {
                    "date": "2016-01-01",
                    "aggregation_unit": "admin3",
                    "daily_location_method": "last",
                    "subscriber_subset": "all",
                },
            },
        ),
        (
            TotalLocationEvents,
            {
                "query_kind": "location_event_counts",
                "params": {
                    "start_date": "2016-01-01",
                    "end_date": "2016-01-02",
                    "interval": "day",
                    "aggregation_unit": "admin3",
                    "direction": "all",
                    "event_types": "all",
                    "subscriber_subset": "all",
                },
            },
        ),
        (
            ModalLocation,
            {
                "query_kind": "modal_location",
                "params": {
                    "locations": (
                        {
                            "query_kind": "daily_location",
                            "params": {
                                "date": "2016-01-01",
                                "aggregation_unit": "admin3",
                                "daily_location_method": "last",
                                "subscriber_subset": "all",
                            },
                        },
                        {
                            "query_kind": "daily_location",
                            "params": {
                                "date": "2016-01-02",
                                "aggregation_unit": "admin3",
                                "daily_location_method": "last",
                                "subscriber_subset": "all",
                            },
                        },
                    ),
                    "aggregation_unit": "admin3",
                },
            },
        ),
        (
            Flows,
            {
                "query_kind": "flows",
                "params": {
                    "from_location": {
                        "query_kind": "daily_location",
                        "params": {
                            "date": "2016-01-01",
                            "aggregation_unit": "admin3",
                            "daily_location_method": "last",
                            "subscriber_subset": "all",
                        },
                    },
                    "to_location": {
                        "query_kind": "daily_location",
                        "params": {
                            "date": "2016-01-02",
                            "aggregation_unit": "admin3",
                            "daily_location_method": "last",
                            "subscriber_subset": "all",
                        },
                    },
                    "aggregation_unit": "admin3",
                },
            },
        ),
        (
            GeoTable,
            {"query_kind": "geography", "params": {"aggregation_unit": "admin3"}},
        ),
    ],
)
def test_construct_query_object_catches_exceptions(
    monkeypatch, query_class, query_spec
):
    """
    Test that construct_query_object catches an exception raised during construction of a query object
    """

    def raise_dummy_exception(*args, **kwargs):
        raise Exception("DUMMY_MESSAGE")

    monkeypatch.setattr(query_class, "__init__", raise_dummy_exception)

    with pytest.raises(QueryProxyError, match="DUMMY_MESSAGE"):
        _ = construct_query_object(**query_spec)
