# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from flowmachine.features import EventCount
from flowmachine.features.nonspatial_aggregates.per_value_aggregate import (
    PerValueAggregate,
)


def test_per_value_aggregate(get_dataframe):
    test_query = EventCount("2016-01-01", "2016-01-02")
    per_val = PerValueAggregate(test_query)
    per_val_df = get_dataframe(per_val)
    test_query_df = get_dataframe(test_query)
    assert all(
        test_query_df.groupby("value").count().rename(columns=dict(subscriber="value"))
        == per_val_df.set_index("grp").sort_index()
    )
