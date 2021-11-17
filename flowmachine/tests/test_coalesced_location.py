# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest

from flowmachine.core.spatial_unit import make_spatial_unit
from flowmachine.core.errors import InvalidSpatialUnitError
from flowmachine.features.subscriber.most_frequent_location import MostFrequentLocation
from flowmachine.features.subscriber.coalesced_location import CoalescedLocation


def test_coalesced_location_column_names(
    get_column_names_from_run, exemplar_spatial_unit_param
):
    """Test that CoalescedLocation's column_names property is accurate"""
    cl = CoalescedLocation(
        preferred_locations=MostFrequentLocation(
            "2016-01-01", "2016-01-02", spatial_unit=exemplar_spatial_unit_param
        ),
        fallback_locations=MostFrequentLocation(
            "2016-01-02", "2016-01-03", spatial_unit=exemplar_spatial_unit_param
        ),
    )
    assert get_column_names_from_run(cl) == cl.column_names


def test_coalesced_location_spatial_unit():
    """Test that CoalescedLocation's spatial unit matches that of its arguments"""
    locations = MostFrequentLocation(
        "2016-01-01", "2016-01-02", spatial_unit=make_spatial_unit("admin", level=1)
    )
    cl = CoalescedLocation(
        preferred_locations=locations,
        fallback_locations=locations,
    )
    assert cl.spatial_unit == locations.spatial_unit


def test_coalesced_location_with_mismatched_spatial_units_raises():
    """
    Test that CoalescedLocation raises InvalidSpatialUnitError if spatial units
    do not match
    """
    with pytest.raises(InvalidSpatialUnitError):
        cl_with_mismatched_spatial_units = CoalescedLocation(
            preferred_locations=MostFrequentLocation(
                "2016-01-01",
                "2016-01-02",
                spatial_unit=make_spatial_unit("versioned-cell"),
            ),
            fallback_locations=MostFrequentLocation(
                "2016-01-02",
                "2016-01-03",
                spatial_unit=make_spatial_unit("admin", level=3),
            ),
        )


def test_coalesced_location_result(get_dataframe):
    """Test that result of CoalescedLocation query is as expected"""
    preferred_locations = MostFrequentLocation(
        "2016-01-01",
        "2016-01-02",
        spatial_unit=make_spatial_unit("admin", level=3),
    )
    fallback_locations = MostFrequentLocation(
        "2016-01-02",
        "2016-01-03",
        spatial_unit=make_spatial_unit("admin", level=3),
    )
    cl = CoalescedLocation(
        preferred_locations=preferred_locations,
        fallback_locations=fallback_locations,
    )
    preferred_locations_df = get_dataframe(preferred_locations).set_index("subscriber")
    fallback_locations_df = get_dataframe(fallback_locations).set_index("subscriber")
    cl_df = get_dataframe(cl).set_index("subscriber")

    # Subscribers appearing in preferred_locations should all have the same location as in preferred_locations
    unfilled_rows = cl_df.join(
        preferred_locations_df, how="inner", rsuffix="_preferred"
    )
    assert (unfilled_rows.pcod == unfilled_rows.pcod_preferred).all()
    # Subscribers not appearing in preferred_locations should all have the same location as in fallback_locations
    filled_rows = cl_df[~cl_df.index.isin(preferred_locations_df.index)].join(
        fallback_locations_df, how="left", rsuffix="_fallback"
    )
    assert (filled_rows.pcod == filled_rows.pcod_fallback).all()
