# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest

from flowmachine.core.sqlalchemy_table_definitions import EventsCallsTable
from flowmachine.core.sqlalchemy_utils import get_string_representation
from flowmachine.core.hour_slice import HourSlice, MultipleHourSlices


def test_daily_hour_slice():
    hs = HourSlice(start_hour="00:00", stop_hour="06:30", freq="day")
    assert hs.start_hour == "00:00"
    assert hs.stop_hour == "06:30"
    assert hs.period.freq == "day"

    ts_col = EventsCallsTable.datetime
    expr = hs.filter_timestamp_column(ts_col)
    expected = "to_char(events.calls.datetime, 'HH24:MI') >= '00:00' AND to_char(events.calls.datetime, 'HH24:MI') < '06:30'"
    assert expected == get_string_representation(expr)


def test_weekly_hour_slice():
    hs = HourSlice(
        start_hour="04:00", stop_hour="07:45", freq="week", weekday="tuesday"
    )
    assert hs.start_hour == "04:00"
    assert hs.stop_hour == "07:45"
    assert hs.period.freq == "week"
    assert hs.period.weekday == "Tuesday"

    ts_col = EventsCallsTable.datetime
    expr = hs.filter_timestamp_column(ts_col)
    expected = (
        "to_char(events.calls.datetime, 'HH24:MI') >= '04:00' AND "
        "to_char(events.calls.datetime, 'HH24:MI') < '07:45' AND "
        "EXTRACT(dow FROM events.calls.datetime) = 2"
    )
    assert expected == get_string_representation(expr)


def test_invalid_arguments():
    with pytest.raises(
        ValueError, match="Argument `freq` must be one of: 'day', 'week'."
    ):
        HourSlice(start_hour="00:00", stop_hour="08:00", freq="foobar")

    with pytest.raises(
        ValueError, match="If freq='week' then the `weekday` argument must be provided."
    ):
        HourSlice(start_hour="00:00", stop_hour="08:00", freq="week", weekday=None)

    with pytest.raises(ValueError, match="Invalid value for `weekday`."):
        HourSlice(start_hour="00:00", stop_hour="08:00", freq="week", weekday="foobar")

    with pytest.raises(
        ValueError,
        match="If freq='day' then the `weekday` argument must not be provided.",
    ):
        HourSlice(start_hour="00:00", stop_hour="08:00", freq="day", weekday="Monday")


def test_multiple_our_slices():
    hs1 = HourSlice(start_hour="08:00", stop_hour="16:30", freq="day")
    hs2 = HourSlice(
        start_hour="10:00", stop_hour="18:45", freq="week", weekday="Thursday"
    )
    mhs = MultipleHourSlices(hour_slices=[hs1, hs2])

    ts_col = EventsCallsTable.datetime
    expr = mhs.filter_timestamp_column(ts_col)
    expected = (
        "to_char(events.calls.datetime, 'HH24:MI') >= '08:00' AND "
        "to_char(events.calls.datetime, 'HH24:MI') < '16:30' OR "
        "to_char(events.calls.datetime, 'HH24:MI') >= '10:00' AND "
        "to_char(events.calls.datetime, 'HH24:MI') < '18:45' AND "
        "EXTRACT(dow FROM events.calls.datetime) = 4"
    )
    assert expected == get_string_representation(expr)
