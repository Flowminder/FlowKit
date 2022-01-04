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
from flowmachine.features.location.meaningful_locations_od import MeaningfulLocationsOD


def test_column_names_meaningful_locations_od(
    exemplar_spatial_unit_param, get_column_names_from_run, meaningful_locations_labels
):
    """Test that column_names property matches head(0) for an od matrix between meaningful locations"""
    if not exemplar_spatial_unit_param.is_polygon:
        pytest.xfail(
            f"The spatial unit {exemplar_spatial_unit_param} is not supported as an aggregation unit for ODs between MeaningfulLocations."
        )
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
        label="evening",
    )

    mfl_b = MeaningfulLocations(
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
    mfl_od = MeaningfulLocationsOD(
        meaningful_locations_a=mfl_a,
        meaningful_locations_b=mfl_b,
        spatial_unit=exemplar_spatial_unit_param,
    )

    assert get_column_names_from_run(mfl_od) == mfl_od.column_names


def test_meaningful_locations_od_raises_for_bad_spatial_unit(
    exemplar_spatial_unit_param, get_dataframe, meaningful_locations_labels
):
    """
    Test that od on meaningful locations raises an InvalidSpatialUnitError for a bad spatial unit.
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
        label="evening",
    )

    with pytest.raises(InvalidSpatialUnitError):
        mfl_od = MeaningfulLocationsOD(
            meaningful_locations_a=mfl,
            meaningful_locations_b=mfl,
            spatial_unit=make_spatial_unit("lon-lat"),
        )


def test_meaningful_locations_od_results(get_dataframe, meaningful_locations_labels):
    """
    Test that OD on MeaningfulLocations returns expected results and counts clusters per subscriber correctly.
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
    mfl_od = MeaningfulLocationsOD(
        meaningful_locations_a=mfl_a,
        meaningful_locations_b=mfl_b,
        spatial_unit=make_spatial_unit("admin", level=1),
    )
    mfl_od_df = get_dataframe(mfl_od)
    # Smoke test one admin1 region gets the expected result
    regional_flow = mfl_od_df[
        (mfl_od_df.pcod_from == "524 1") & (mfl_od_df.pcod_to == "524 4")
    ].value.tolist()[0]
    assert regional_flow == pytest.approx(16.490_807)
    assert mfl_od_df.value.sum() == pytest.approx(454.0)
