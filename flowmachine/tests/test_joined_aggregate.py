# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import warnings

import pytest

from flowmachine.core import make_spatial_unit
from flowmachine.features import (
    MostFrequentLocation,
    RadiusOfGyration,
    SubscriberDegree,
)


def test_joined_aggregate(get_dataframe):
    """
    Test join aggregate.
    """
    mfl = MostFrequentLocation(
        "2016-01-01", "2016-01-04", spatial_unit=make_spatial_unit("admin", level=3)
    )
    joined = mfl.join_aggregate(RadiusOfGyration("2016-01-01", "2016-01-04"))
    assert (
        pytest.approx(198.32613522641248)
        == get_dataframe(joined).set_index("pcod").loc["524 2 05 29"].value
    )


def test_joined_modal_aggregate(get_dataframe):
    """
    Test join with modal aggregate.
    """
    mfl = MostFrequentLocation(
        "2016-01-01", "2016-01-04", spatial_unit=make_spatial_unit("admin", level=3)
    )
    rog = SubscriberDegree("2016-01-01", "2016-01-04")
    joined = mfl.join_aggregate(rog, method="mode")
    rawus_mode = (
        get_dataframe(rog)
        .set_index("subscriber")
        .join(get_dataframe(mfl).set_index("subscriber"))
        .set_index("pcod")
        .loc["524 2 05 29"]
        .value.mode()[0]
    )
    assert (
        pytest.approx(rawus_mode)
        == get_dataframe(joined).set_index("pcod").loc["524 2 05 29"].value
    )


def test_joined_median_aggregate(get_dataframe):
    """
    Test join with median aggregate.
    """
    mfl = MostFrequentLocation(
        "2016-01-01", "2016-01-04", spatial_unit=make_spatial_unit("admin", level=3)
    )
    rog = RadiusOfGyration("2016-01-01", "2016-01-04")
    joined = mfl.join_aggregate(rog, method="median")
    rawus_avg = (
        get_dataframe(rog)
        .set_index("subscriber")
        .join(get_dataframe(mfl).set_index("subscriber"))
        .set_index("pcod")
        .loc["524 2 05 29"]
        .value.median()
    )
    assert (
        pytest.approx(rawus_avg)
        == get_dataframe(joined).set_index("pcod").loc["524 2 05 29"].value
    ), rawus_avg


def test_joined_agg_date_mismatch():
    """
    Test that join aggregate with mismatched dates raises a warning.
    """
    mfl = MostFrequentLocation(
        "2016-01-01", "2016-01-04", spatial_unit=make_spatial_unit("admin", level=3)
    )
    with pytest.warns(UserWarning):
        mfl.join_aggregate(RadiusOfGyration("2016-01-02", "2016-01-04"))

    with pytest.warns(UserWarning):
        mfl.join_aggregate(RadiusOfGyration("2016-01-01", "2016-01-05"))


def test_joined_agg_hours_mismatch():
    """
    Test that join aggregate with mismatched hours doesn't warn.
    """
    mfl = MostFrequentLocation(
        "2016-01-01 10:00",
        "2016-01-04",
        spatial_unit=make_spatial_unit("admin", level=3),
    )
    with warnings.catch_warnings(record=True) as w:
        mfl.join_aggregate(RadiusOfGyration("2016-01-01", "2016-01-04"))
        assert not w
