# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest

from flowmachine.core.errors import MissingDateError
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


def test_works_with_pcods(get_dataframe):
    """
    We can get daily locations with p-codes rather than the standard names.
    """

    dl = daily_location("2016-01-05", level="admin3", column_name="admin3pcod")
    df = get_dataframe(dl)
    assert df.admin3pcod[0].startswith("524")


def test_hours(get_length):
    """
    Test that daily locations handles the hours parameter
    """

    # Lower level test test that subsetdates handles this correctly
    # we're just testing that it is passed on in this case.

    dl1 = daily_location("2016-01-01", level="cell")
    dl2 = daily_location("2016-01-01", level="cell", hours=(19, 23))
    dl3 = daily_location("2016-01-01", level="cell", hours=(19, 20))

    assert get_length(dl1) > get_length(dl2) > get_length(dl3)


def test_daily_locs_errors():
    """
    daily_location() errors when we ask for a date that does not exist.
    """

    with pytest.raises(MissingDateError):
        daily_location("2016-01-31")
