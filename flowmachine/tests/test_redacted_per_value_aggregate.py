# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from flowmachine.features import EventCount
from flowmachine.features.nonspatial_aggregates.redacted_per_value_aggregate import (
    RedactedPerValueAggregate,
)


def test_redacted_per_value_aggregate(get_dataframe):
    test_query = EventCount("2016-01-01", "2016-01-02")
    per_val = RedactedPerValueAggregate(test_query)
    per_val_df = get_dataframe(per_val)
    test_query_df = get_dataframe(test_query)
    aggregated_with_pandas = (
        test_query_df.groupby("value").count().rename(columns=dict(subscriber="value"))
    ).query("value > 15")
    assert all(per_val_df.set_index("grp").value > 15)
    assert all(aggregated_with_pandas == per_val_df.set_index("grp").sort_index())
