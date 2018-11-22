# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flowmachine.features import daily_location, HomeLocation
from flowmachine.utils import list_of_dates


def test_inferred_start():
    """
    The start datetime is correctly inferred from a list of locations.
    """
    dls = [
        daily_location(
            "2016-01-01 18:00:00", stop="2016-01-02 06:00:00", method="most-common"
        ),
        daily_location(
            "2016-01-02 18:00:00", stop="2016-01-03 06:00:00", method="most-common"
        ),
        daily_location(
            "2016-01-03 18:00:00", stop="2016-01-04 06:00:00", method="most-common"
        ),
    ]
    hl = HomeLocation(*dls)
    assert "2016-01-01 18:00:00" == hl.start


def test_inferred_start_shuffled():
    """
    The start datetime is correctly inferred from a disordered list of locations.
    """
    dls = [
        daily_location(
            "2016-01-01 18:00:00", stop="2016-01-02 06:00:00", method="most-common"
        ),
        daily_location(
            "2016-01-02 18:00:00", stop="2016-01-03 06:00:00", method="most-common"
        ),
        daily_location(
            "2016-01-03 18:00:00", stop="2016-01-04 06:00:00", method="most-common"
        ),
    ]
    hl = HomeLocation(*dls[::-1])
    assert "2016-01-01 18:00:00" == hl.start


def test_selected_values(get_dataframe):
    """
    HomeLocation() values are correct.
    """
    hdf = get_dataframe(
        HomeLocation(
            *[daily_location(d) for d in list_of_dates("2016-01-01", "2016-01-03")]
        )
    ).set_index("subscriber")

    assert "Dolpa" == hdf.ix["038OVABN11Ak4W5P"][0]
    assert "Baglung" == hdf.ix["E1n7JoqxPBjvR5Ve"][0]
    assert "Myagdi" == hdf.ix["gkBLe0mN5j3qmRpX"][0]
    assert "Kapilbastu" == hdf.ix["5Kgwy8Gp6DlN3Eq9"][0]
