# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


from flowmachine_core.utility_queries.spatial_aggregate import SpatialAggregate


def test_can_be_aggregated(get_dataframe, redactable_locations):
    """
    Query can be aggregated to a spatial level with lon-lat data.
    """
    agg = SpatialAggregate(locations=redactable_locations)
    df = get_dataframe(agg)
    assert df.set_index("pcod").loc["a"].values == 20
    assert df.set_index("pcod").loc["b"].values == 6
