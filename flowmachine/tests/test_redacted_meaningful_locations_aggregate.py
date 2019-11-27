# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flowmachine.core import make_spatial_unit
from flowmachine.features import (
    MeaningfulLocations,
    HartiganCluster,
    CallDays,
    SubscriberLocations,
    EventScore,
)
from flowmachine.features.location.meaningful_locations_aggregate import (
    MeaningfulLocationsAggregate,
)
from flowmachine.features.location.redacted_meaningful_locations_aggregate import (
    RedactedMeaningfulLocationsAggregate,
)

labels = {
    "evening": {
        "type": "Polygon",
        "coordinates": [[[1e-06, -0.5], [1e-06, -1.1], [1.1, -1.1], [1.1, -0.5]]],
    },
    "day": {
        "type": "Polygon",
        "coordinates": [[[-1.1, -0.5], [-1.1, 0.5], [-1e-06, 0.5], [0, -0.5]]],
    },
}


def test_meaningful_locations_aggregation_results(
    exemplar_spatial_unit_param, get_dataframe
):
    """
    Test that aggregating MeaningfulLocations returns expected results and redacts values below 15.
    """
    mfl = MeaningfulLocations(
        clusters=HartiganCluster(
            calldays=CallDays(
                subscriber_locations=SubscriberLocations(
                    start="2016-01-01",
                    stop="2016-01-02",
                    spatial_unit=make_spatial_unit("versioned-site"),
                )
            ),
            radius=1,
        ),
        scores=EventScore(
            start="2016-01-01",
            stop="2016-01-02",
            spatial_unit=make_spatial_unit("versioned-site"),
        ),
        labels=labels,
        label="evening",
    )
    mfl_agg = RedactedMeaningfulLocationsAggregate(
        meaningful_locations_aggregate=MeaningfulLocationsAggregate(
            meaningful_locations=mfl, spatial_unit=make_spatial_unit("admin", level=3)
        )
    )
    mfl_agg_df = get_dataframe(mfl_agg)
    # Aggregate should not include any counts below 15
    assert all(mfl_agg_df.total > 15)
