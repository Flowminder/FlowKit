# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-

from flowmachine.features import DayTrajectories, daily_location
from flowmachine.features.location.majority_location import MajorityLocation
from flowmachine.features.subscriber.location_visits import LocationVisits
from pandas import DataFrame as df
from pandas.testing import assert_frame_equal

import pytest


@pytest.fixture
def location_visits(flowmachine_connect):
    lv = LocationVisits(
        DayTrajectories(
            daily_location("2016-01-01"),
            daily_location("2016-01-02"),
            daily_location("2016-01-03"),
            daily_location("2016-01-04"),
        )
    )
    yield lv


def test_majority_location(get_dataframe, location_visits):
    lv = location_visits
    ml = MajorityLocation(subscriber_location_weights=lv, weight_column="dl_count")
    out = get_dataframe(ml)
    assert len(out) == 15
    assert out.subscriber.is_unique
    target = df.from_records(
        [
            ["1QBlwRo4Kd5v3Ogz", "524 3 08 44"],
            ["37J9rKydzJ0mvo0z", "524 4 12 62"],
            ["3XKdxqvyNxO2vLD1", "524 4 12 62"],
            ["81B6q0K8k325XWmn", "524 1 03 13"],
            ["8dXPM6JXj05qwjW0", "524 4 12 62"],
            ["bKZLwjrMQG7z468y", "524 4 12 62"],
            ["g9D5KEK9BQzOa8z0", "524 4 11 57"],
        ],
        columns=["subscriber", "pcod"],
    )
    assert (ml.column_names == target.columns).all()
    assert_frame_equal(out.head(7), target)


def test_include_unlocatable(get_dataframe, location_visits):
    lv = location_visits
    ml = MajorityLocation(
        subscriber_location_weights=lv,
        weight_column="dl_count",
        include_unlocatable=True,
    )
    out_ml = get_dataframe(ml)
    out_lv = get_dataframe(lv)
    assert out_lv.subscriber.nunique() == len(out_ml)
    assert len(out_ml.pcod.dropna()) == 15


def test_weight_column_missing(location_visits):
    lv = location_visits
    with pytest.raises(ValueError):
        ml = MajorityLocation(
            subscriber_location_weights=lv, weight_column="nonexistant"
        )


def test_spatial_units(exemplar_spatial_unit_param, get_dataframe):
    lv = LocationVisits(
        DayTrajectories(
            daily_location("2016-01-01", spatial_unit=exemplar_spatial_unit_param),
            daily_location("2016-01-02", spatial_unit=exemplar_spatial_unit_param),
        )
    )
    ml = MajorityLocation(subscriber_location_weights=lv, weight_column="dl_count")
    assert ml.column_names == (["subscriber"] + lv.spatial_unit.location_id_columns)
    out = get_dataframe(ml)
    assert len(out) >= 0
