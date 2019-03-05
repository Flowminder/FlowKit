# This Sour:e Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests for the class FirstLocation
"""

from flowmachine.features.subscriber import FirstLocation


def test_time_at_first_location_correct(get_dataframe):
    """
    FirstLocation() dataframe contains hand-picked records.
    """
    dfl = FirstLocation(
        "2016-01-01", "2016-01-04", location="QeBRM8", level="versioned-site"
    )
    df = get_dataframe(dfl)

    set_df = df.set_index("subscriber")
    assert str(set_df.loc["038OVABN11Ak4W5P"]) == "2016-01-01 05:02:10+00:00"
    assert str(set_df.loc["1p4MYbA1Y4bZzBQa"]) == "2016-01-02 21:30:41+00:00"
    assert str(set_df.loc["3XKdxqvyNxO2vLD1"]) == "2016-01-01 05:09:20+00:00"


def test_handles_list_of_locations(get_dataframe):
    """
    FirstLocation() subsets data based on a list of locations, rather than a single one.
    """
    dfl = FirstLocation(
        "2016-01-01",
        "2016-01-04",
        location=["QeBRM8", "m9jL23" "LVnDQL"],
        level="versioned-site",
    )
    df = get_dataframe(dfl)

    df.set_index("subscriber", inplace=True)
    assert str(df.loc["038OVABN11Ak4W5P"]) == "2016-01-01 05:02:10+00:00"


def test_can_be_called_with_any(get_dataframe):
    """
    FirstLocation() can call first at location with the keyword 'any'.
    """
    dfl = FirstLocation("2016-01-03", "2016-01-04", location="any")
    df = get_dataframe(dfl)
    df.set_index("subscriber", inplace=True)
    assert str(df.loc["0MQ4RYeKn7lryxGa"]) == "2016-01-03 01:38:56+00:00"
