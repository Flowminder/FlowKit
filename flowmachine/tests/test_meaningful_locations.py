# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import pytest

from flowmachine.core import make_spatial_unit
from flowmachine.features import (
    HartiganCluster,
    CallDays,
    SubscriberLocations,
    EventScore,
)
from flowmachine.features.subscriber.meaningful_locations import MeaningfulLocations


def test_column_names_meaningful_locations(
    get_column_names_from_run, meaningful_locations_labels
):
    """Test that column_names property matches head(0) for meaningfullocations"""
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
        labels=meaningful_locations_labels,
        label="evening",
    )

    assert get_column_names_from_run(mfl) == mfl.column_names


@pytest.mark.parametrize(
    "label, expected_number_of_clusters",
    [("evening", 702), ("day", 0), ("unknown", 1615)],
)
def test_meaningful_locations_results(
    label, expected_number_of_clusters, get_dataframe, meaningful_locations_labels
):
    """
    Test that MeaningfulLocations returns expected results and counts clusters per subscriber correctly.
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
        labels=meaningful_locations_labels,
        label=label,
    )
    mfl_df = get_dataframe(mfl)
    assert len(mfl_df) == expected_number_of_clusters
    count_clusters = mfl_df.groupby(
        ["subscriber", "label", "n_clusters"], as_index=False
    ).count()
    # Check that query has correctly counted the number of clusters per subscriber
    assert all(count_clusters.n_clusters == count_clusters.cluster)
