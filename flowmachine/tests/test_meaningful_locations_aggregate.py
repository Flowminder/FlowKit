# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest

from flowmachine.core import make_spatial_unit
from flowmachine.core.errors import InvalidSpatialUnitError
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


def test_column_names_meaningful_locations_aggregate(
    exemplar_spatial_unit_param, get_column_names_from_run, meaningful_locations_labels
):
    """Test that column_names property matches head(0) for aggregated meaningful locations"""
    if not exemplar_spatial_unit_param.is_polygon:
        pytest.xfail(
            f"The spatial unit {exemplar_spatial_unit_param} is not supported as an aggregation unit for MeaningfulLocations."
        )
    mfl_agg = MeaningfulLocationsAggregate(
        meaningful_locations=MeaningfulLocations(
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
        ),
        spatial_unit=exemplar_spatial_unit_param,
    )

    assert get_column_names_from_run(mfl_agg) == mfl_agg.column_names


def test_meaningful_locations_aggregate_disallowed_spatial_unit_raises(
    meaningful_locations_labels,
):
    """Test that a bad spatial unit raises an InvalidSpatialUnitError"""

    with pytest.raises(InvalidSpatialUnitError):
        mfl_agg = MeaningfulLocationsAggregate(
            meaningful_locations=MeaningfulLocations(
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
            ),
            spatial_unit=make_spatial_unit("lon-lat"),
        )


def test_meaningful_locations_aggregation_results(
    exemplar_spatial_unit_param, get_dataframe, meaningful_locations_labels
):
    """
    Test that aggregating MeaningfulLocations returns expected results.
    """
    if not exemplar_spatial_unit_param.is_polygon:
        pytest.xfail(
            f"The spatial unit {exemplar_spatial_unit_param} is not supported as an aggregation unit for MeaningfulLocations."
        )
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
    mfl_agg = MeaningfulLocationsAggregate(
        meaningful_locations=mfl, spatial_unit=exemplar_spatial_unit_param
    )
    mfl_df = get_dataframe(mfl)
    mfl_agg_df = get_dataframe(mfl_agg)

    # Sum of aggregate should equal to the number of unique subscribers
    assert mfl_agg_df.value.sum() == pytest.approx(mfl_df.subscriber.nunique())
