# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
import pytest

from flowmachine.features.utilities.unique_values_from_queries import (
    UniqueValuesFromQueries,
)
from flowmachine.features.utilities.events_tables_union import EventTableSubset
from flowmachine.core.errors import MissingColumnsError


def test_unique_subscribers_from_queries(get_dataframe):

    one_column_query = UniqueValuesFromQueries(
        query_list=EventTableSubset(start="2016-01-01", stop="2016-01-03"),
        column_names="location_id",
    )
    two_column_query = UniqueValuesFromQueries(
        query_list=[EventTableSubset(start="2016-01-01", stop="2016-01-03")],
        column_names=["imei", "location_id"],
    )
    print(get_dataframe(two_column_query))
    assert get_dataframe(two_column_query).iloc[4].tolist() == [
        "097b5f121396c77d79150acae9a3052d",
        "b80699f8af4fa963c6f75eb6990556e1",
    ]
    assert get_dataframe(two_column_query).columns.tolist() == ["imei", "location_id"]
    # location_id should dedupe more than location_id and imei, which should in turn dedupe more that the intitial query
    assert (
        len(get_dataframe(one_column_query))
        < len(get_dataframe(two_column_query))
        < len(get_dataframe(EventTableSubset(start="2016-01-01", stop="2016-01-03")))
    )


def test_missing_columns_exception(get_dataframe):
    with pytest.raises(MissingColumnsError):
        failing_query = UniqueValuesFromQueries(
            query_list=[EventTableSubset(start="2016-01-01", stop="2016-01-03")],
            column_names=["foo", "bar"],
        )
