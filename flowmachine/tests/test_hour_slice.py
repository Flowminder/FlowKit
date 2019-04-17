# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import pytest

from flowmachine.core.sqlalchemy_table_definitions import EventsCallsTable
from flowmachine.core.sqlalchemy_utils import get_string_representation
from flowmachine.core.hour_slice import HourSlice


def test_daily_hour_slice():
    hs = HourSlice(start="00:00", stop="06:30", freq="day")
    assert hs.start == "00:00"
    assert hs.stop == "06:30"
    assert hs.period.freq == "day"

    ts_col = EventsCallsTable.datetime
    expr = hs.filter_timestamp_column(ts_col)
    expected = "to_char(events.calls.datetime, 'HH24:MI') >= '00:00' AND to_char(events.calls.datetime, 'HH24:MI') < '06:30'"
    assert expected == get_string_representation(expr)


def test_weekly_hour_slice():
    hs = HourSlice(start="04:00", stop="07:45", freq="week", weekday="Tuesday")
    assert hs.start == "04:00"
    assert hs.stop == "07:45"
    assert hs.period.freq == "week"
    assert hs.period.weekday == "Tuesday"

    ts_col = EventsCallsTable.datetime
    expr = hs.filter_timestamp_column(ts_col)
    expected = (
        "to_char(events.calls.datetime, 'HH24:MI') >= '04:00' AND "
        "to_char(events.calls.datetime, 'HH24:MI') < '07:45' AND "
        "EXTRACT(dow FROM events.calls.datetime) = 'Tuesday'"
    )
    assert expected == get_string_representation(expr)


def test_invalid_arguments():
    with pytest.raises(
        ValueError, match="Argument `freq` must be one of: 'day', 'week'."
    ):
        HourSlice(start="00:00", stop="08:00", freq="foobar")

    with pytest.raises(
        ValueError, match="If freq='week' then `weekday` must be given."
    ):
        HourSlice(start="00:00", stop="08:00", freq="week", weekday=None)

    with pytest.raises(ValueError, match="Invalid value for `weekday`."):
        HourSlice(start="00:00", stop="08:00", freq="week", weekday="foobar")

    with pytest.raises(
        ValueError, match="If freq='day' then `weekday` must not be given."
    ):
        HourSlice(start="00:00", stop="08:00", freq="day", weekday="Monday")
