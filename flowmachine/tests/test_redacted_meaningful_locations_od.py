# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flowmachine.core import make_spatial_unit
from flowmachine.features import (
    CallDays,
    EventScore,
    HartiganCluster,
    MeaningfulLocations,
    SubscriberLocations,
)
from flowmachine.features.location.meaningful_locations_od import MeaningfulLocationsOD
from flowmachine.features.location.redacted_meaningful_locations_od import (
    RedactedMeaningfulLocationsOD,
)


def test_meaningful_locations_od_redaction(get_dataframe, meaningful_locations_labels):
    """
    Test that OD on MeaningfulLocations is redacted to >15.
    """

    mfl_a = MeaningfulLocations(
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
        labels=meaningful_locations_labels,
        label="unknown",
    )

    mfl_b = MeaningfulLocations(
        clusters=HartiganCluster(
            calldays=CallDays(
                subscriber_locations=SubscriberLocations(
                    start="2016-01-02",
                    stop="2016-01-03",
                    spatial_unit=make_spatial_unit("versioned-site"),
                )
            ),
            radius=1,
        ),
        scores=EventScore(
            start="2016-01-02",
            stop="2016-01-03",
            spatial_unit=make_spatial_unit("versioned-site"),
        ),
        labels=meaningful_locations_labels,
        label="unknown",
    )
    mfl_od = RedactedMeaningfulLocationsOD(
        meaningful_locations_od=MeaningfulLocationsOD(
            meaningful_locations_a=mfl_a,
            meaningful_locations_b=mfl_b,
            spatial_unit=make_spatial_unit("admin", level=1),
        )
    )
    mfl_od_df = get_dataframe(mfl_od)
    # Aggregate should not include any counts below 15
    assert all(mfl_od_df.value > 15)
