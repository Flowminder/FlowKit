# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-

from flowmachine.features.utilities.unique_values_from_queries import (
    UniqueValuesFromQueries,
)
from flowmachine.features.utilities.events_tables_union import EventTableSubset


def test_unique_subscribers_from_queries(get_dataframe):
    unique_subscriber_query = UniqueValuesFromQueries(
        [EventTableSubset(start="2016-01-01", stop="2016-01-03")], ["imei", "imsi"]
    )
    # Using flowdb_synthetic_data (testdata seems to be empty?)
    print(get_dataframe(unique_subscriber_query))
    assert get_dataframe(unique_subscriber_query).iloc[4].tolist() == [
        "c38c773ae151c0573209c7697f6ba24a",
        "52cb2c1a142e1c86b8f79d8783535fb8",
    ]
    assert get_dataframe(unique_subscriber_query).columns.tolist() == ["imei", "imsi"]
