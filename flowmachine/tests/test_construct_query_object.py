# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest

from flowmachine.core import GeoTable
from flowmachine.features import LastLocation, ModalLocation, Flows, TotalLocationEvents

from flowmachine.core.server.query_proxy import (
    construct_query_object,
    QueryProxyError,
    InvalidGeographyError,
)


@pytest.mark.parametrize(
    "expected_md5, query_spec",
    [
        (
            "5b2b2484e941da429b52dda7e81fb917",
            {"query_kind": "geography", "params": {"aggregation_unit": "admin3"}},
        )
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
        obj = construct_query_object(
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
        obj = construct_query_object(**query_spec)
