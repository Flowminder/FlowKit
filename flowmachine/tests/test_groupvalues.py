# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Tests for the group values API.
"""

from flowmachine.features import GroupValues


def test_can_group():
    """
    Test that we can group the data by a column and
    return an iterator.
    """

    gv = GroupValues("subscriber", "datetime", "2016-01-01", "2016-01-03")
    for line in gv:
        assert len(line) == 2
        break


def test_can_map():
    """
    Test that we can map a function onto our map.
    """

    def highest_min(date_list):
        """
        Takes a list of dates and returns the highest min.
        """

        return max([x.minute for x in date_list])

    gv = GroupValues("subscriber", "datetime", "2016-01-01", "2016-01-03")
    cm = gv.ColumnMap(highest_min)
    for line in cm:
        assert len(line) == 2


def test_can_map_multiple_groups():
    """
    Can get an iterator from flowmachine.GroupValues with multiple groups
    """

    gv = GroupValues(
        ["subscriber", "msisdn_counterpart"], "datetime", "2016-01-01", "2016-01-03"
    )
    for line in gv:
        assert len(line) == 3
        break


def test_can_apply_function_to_multiple_values():
    """
    Can apply reducing function to more than one value
    """

    def longest_call(start, durations):
        """
        Returns the longest call, given ordered arrays of start and stop times.
        """
        durations = [
            start.second + duration for start, duration in zip(start, durations)
        ]
        return int(max(durations))

    gv = GroupValues("subscriber", ["datetime", "duration"], "2016-01-01", "2016-01-03")
    cm = gv.ColumnMap(longest_call)
    for groups, answer in cm:
        assert len(groups) == 1
        assert isinstance(answer, int)
