# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest

from flowmachine.core.errors import MissingDateError
from flowmachine.core import make_spatial_unit
from flowmachine.core.errors.flowmachine_errors import PreFlightFailedException
from flowmachine.features import daily_location, MostFrequentLocation


def test_equivalent_to_locate_subscribers(get_dataframe):
    """
    daily_location() is equivalent to the MostFrequentLocation().
    """
    mfl = MostFrequentLocation("2016-01-01", "2016-01-02")
    mfl_df = get_dataframe(mfl)

    dl = daily_location("2016-01-01", method="most-common")
    dl_df = get_dataframe(dl)

    assert (dl_df == mfl_df).all().all()


def test_equivalent_to_locate_subscribers_with_time(get_dataframe):
    """
    daily_location() is equivalent to the MostFrequentLocation() with timestamps.
    """
    mfl = MostFrequentLocation("2016-01-01 18:00:00", "2016-01-02 06:00:00")
    mfl_df = get_dataframe(mfl)

    dl = daily_location(
        "2016-01-01 18:00:00", stop="2016-01-02 06:00:00", method="most-common"
    )
    dl_df = get_dataframe(dl)

    assert (dl_df == mfl_df).all().all()


def test_works_with_admin_names(get_dataframe):
    """
    We can get daily locations with admin names rather than pcodes.
    """

    dl = daily_location(
        "2016-01-05",
        spatial_unit=make_spatial_unit(
            "admin", level=3, region_id_column_name="admin3name"
        ),
    )
    df = get_dataframe(dl)
    assert "Lamjung" == df.admin3name[0]


def test_hours(get_length):
    """
    Test that daily locations handles the hours parameter
    """

    # Lower level test test that subsetdates handles this correctly
    # we're just testing that it is passed on in this case.

    dl1 = daily_location("2016-01-01", spatial_unit=make_spatial_unit("cell"))
    dl2 = daily_location(
        "2016-01-01", spatial_unit=make_spatial_unit("cell"), hours=(19, 23)
    )
    dl3 = daily_location(
        "2016-01-01", spatial_unit=make_spatial_unit("cell"), hours=(19, 20)
    )

    assert get_length(dl1) > get_length(dl2) > get_length(dl3)


@pytest.mark.check_available_dates
def test_daily_locs_errors():
    """
    daily_location() errors when we ask for a date that does not exist.
    """

    with pytest.raises(PreFlightFailedException) as exc:
        daily_location("2016-01-31").preflight()
    with pytest.raises(MissingDateError):
        raise exc.value.errors[
            "<Query of type: EventTableSubset, query_id: '607268fcdebd1e536d30256ee0ee40e1'>"
        ][0]
    with pytest.raises(MissingDateError):
        raise exc.value.errors[
            "<Query of type: EventTableSubset, query_id: '8533ca0424a0030218ec75f691c6d465'>"
        ][0]
