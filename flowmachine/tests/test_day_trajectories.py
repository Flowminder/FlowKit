# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


from flowmachine.features import DayTrajectories, daily_location


def test_column_names_day_trajectories(exemplar_level_param):
    """ Test that column_names property matches head(0) for DayTrajectories"""
    lv = DayTrajectories(daily_location("2016-01-01", **exemplar_level_param))
    assert lv.head(0).columns.tolist() == lv.column_names


def test_day_trajectories(get_dataframe):
    """
    DailyLocations calculations within DayTrajectories.get_dataframe() are correct.
    """
    traj = DayTrajectories(daily_location("2016-01-01", level="admin3", method="last"))
    df = get_dataframe(traj).drop("date", axis=1)
    dldf = daily_location("2016-01-01", level="admin3", method="last").get_dataframe()
    assert [df["subscriber"][0], df["name"][0]] == [
        dldf["subscriber"][0],
        dldf["name"][0],
    ]
