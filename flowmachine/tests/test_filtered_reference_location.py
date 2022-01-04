# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import pytest

from flowmachine.features import LocationVisits, daily_location, DayTrajectories
from flowmachine.features.subscriber.filtered_reference_location import (
    FilteredReferenceLocation,
)
from flowmachine.utils import list_of_dates
from flowmachine.core import make_spatial_unit


def test_column_names_filtered_reference_location(exemplar_spatial_unit_param):
    """Test that column_names property matches head(0) for LocationVisits"""
    lv = LocationVisits(
        DayTrajectories(
            daily_location("2016-01-01", spatial_unit=exemplar_spatial_unit_param),
            daily_location("2016-01-02", spatial_unit=exemplar_spatial_unit_param),
        )
    )
    reference = daily_location("2016-01-01", spatial_unit=exemplar_spatial_unit_param)
    filtered = FilteredReferenceLocation(
        reference_locations_query=reference,
        filter_query=lv,
        lower_bound=1,
        upper_bound=2,
    )
    assert filtered.head(0).columns.tolist() == filtered.column_names


def test_value_sum_equal_or_less_than_period(get_dataframe):
    """
    Reference location is filtered by the number of visits.
    """
    start_date = "2016-01-01"
    stop_date = "2016-01-07"
    lv = LocationVisits(
        DayTrajectories(
            *[
                daily_location(
                    d, spatial_unit=make_spatial_unit("admin", level=3), method="last"
                )
                for d in list_of_dates(start_date, stop_date)
            ]
        )
    )
    reference = daily_location(
        "2016-01-01", spatial_unit=make_spatial_unit("admin", level=3), method="last"
    )
    filtered = FilteredReferenceLocation(
        reference_locations_query=reference,
        filter_query=lv,
        lower_bound=2,
        upper_bound=2,
    )
    filtered_df = get_dataframe(filtered)
    counts_df = get_dataframe(lv)
    assert "038OVABN11Ak4W5P" in filtered_df.subscriber.values
    assert "09NrjaNNvDanD8pk" not in filtered_df.subscriber.values
    assert all(
        (
            filtered_df.set_index(["subscriber", "pcod"])
            .join(counts_df.set_index(["subscriber", "pcod"]))
            .values
            > 1
        )
    )


def test_unit_mismatch_raises():
    """
    Mismatched spatial units raise an error
    """
    start_date = "2016-01-01"
    stop_date = "2016-01-07"
    lv = LocationVisits(
        DayTrajectories(
            *[
                daily_location(
                    d, spatial_unit=make_spatial_unit("admin", level=3), method="last"
                )
                for d in list_of_dates(start_date, stop_date)
            ]
        )
    )
    reference = daily_location(
        "2016-01-01", spatial_unit=make_spatial_unit("admin", level=1), method="last"
    )
    with pytest.raises(
        ValueError,
        match="reference_locations_query and filter_query must have a common spatial unit.",
    ):
        FilteredReferenceLocation(
            reference_locations_query=reference,
            filter_query=lv,
            lower_bound=2,
            upper_bound=2,
        )
