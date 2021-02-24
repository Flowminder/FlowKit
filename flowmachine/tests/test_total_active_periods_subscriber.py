# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests for the class flowmachine.TotalActivePeriodsSubscriber
"""
import pytest

from flowmachine.features import TotalActivePeriodsSubscriber


def test_certain_results(get_dataframe):
    """
    flowmachine.TotalActivePeriodsSubscriber gives correct results.
    """
    tap = TotalActivePeriodsSubscriber("2016-01-01", 3, 1)
    # This subscriber should have only 2 active time periods
    subscriber_with_2 = "rmb4PG9gx1ZqBE01"
    # and this one three
    subscriber_with_3 = "dXogRAnyg1Q9lE3J"

    df = get_dataframe(tap).set_index("subscriber")
    assert df.loc[subscriber_with_2].value == 2
    assert df.loc[subscriber_with_3].value == 3
    assert df.loc[subscriber_with_2].inactive_periods == 1
    assert df.loc[subscriber_with_3].inactive_periods == 0


def test_multiple_day_periods(get_dataframe):
    """
    flowmachine.TotalActivePeriodsSubscriber can handle a period
    greater than one day.
    """

    tap = TotalActivePeriodsSubscriber("2016-01-02", 3, 2)
    df = get_dataframe(tap)
    starts = ["2016-01-02 00:00:00", "2016-01-04 00:00:00", "2016-01-06 00:00:00"]
    stops = ["2016-01-04 00:00:00", "2016-01-06 00:00:00", "2016-01-08 00:00:00"]
    # Check that the start and stop dates are as expected
    assert tap.starts == starts
    assert tap.stops == stops
    # For good measure assert that no subscriber has more than the
    # max number of periods
    assert df.value.max() == 3
    assert df.inactive_periods.max() == 0


def test_raises_value_error_bad_unit():
    """
    flowmachine.TotalActivePeriodsSubscriber raises value error when
    we pass a none-allowed time unit.
    """

    with pytest.raises(ValueError):
        TotalActivePeriodsSubscriber("2016-01-01", 5, period_unit="microfortnight")


def test_non_standard_units(get_dataframe):
    """
    flowmachine.TotalActivePeriodsSubscriber is able to handle a period_unit other
    than the default 'days'.
    """

    df = (
        TotalActivePeriodsSubscriber("2016-01-01", 5, period_unit="hours")
        .get_dataframe()
        .set_index("subscriber")
    )

    assert df.loc["VkzMxYjv7mYn53oK"].value == 3
    assert df.loc["DzpZJ2EaVQo2X5vM"].value == 1
    assert df.loc["VkzMxYjv7mYn53oK"].inactive_periods == 2
    assert df.loc["DzpZJ2EaVQo2X5vM"].inactive_periods == 4
