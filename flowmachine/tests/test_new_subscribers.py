# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Unit tests for the Query() base class.
"""


from flowmachine.features import NewSubscribers


def test_has_right_columns(get_dataframe):
    """
    NewSubscribers() returned dataframe has the right column..
    """
    nu = NewSubscribers("2016-01-01", "2016-01-03", "2016-01-05", "2016-01-07")
    assert nu.column_names == ["subscriber"]


def test_specific_values(get_dataframe):
    """
    NewSubscribers() results match a few know values from the query that we have.
    """
    UU = NewSubscribers(
        "2016-01-01 01:00:00", "2016-01-02 02:00:00", "2016-01-03", "2016-01-04"
    )
    df = get_dataframe(UU)
    nus = set(df.subscriber)
    assert "BGXbdrXG9J2gVZoA" in nus
    assert "Va0P7xJQVOmMpQn6" in nus
    assert "148ZaRZe54wPGQ9r" not in nus
