# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-

import pytest
from pandas import DataFrame as df
from pandas.testing import assert_frame_equal

from flowmachine.core.custom_query import CustomQuery
from flowmachine.core.spatial_unit import CellSpatialUnit
from flowmachine.features import DayTrajectories, daily_location
from flowmachine.features.location.total_events import TotalLocationEvents
from flowmachine.features.subscriber.handset_stats import HandsetStats
from flowmachine.features.subscriber.location_visits import LocationVisits
from flowmachine.features.subscriber.majority_location import majority_location


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
    ml = majority_location(
        subscriber_location_weights=lv, weight_column="value", include_unlocatable=False
    )
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
    assert (ml.column_names == out.columns).all()
    assert_frame_equal(out.head(7), target)


def test_include_unlocatable(get_dataframe, location_visits):
    lv = location_visits
    ml = majority_location(
        subscriber_location_weights=lv,
        weight_column="value",
        include_unlocatable=True,
    )
    out_ml = get_dataframe(ml)
    out_lv = get_dataframe(lv)
    assert (ml.column_names == out_ml.columns).all()
    assert out_lv.subscriber.nunique() == len(out_ml)
    assert len(out_ml.pcod.dropna()) == 15


def test_include_unlocatable_dependency(location_visits):
    """
    Test that a majority_location query with include_unlocatable=True
    depends on the corresponding query with include_unlocatable=False
    """
    ml_false = majority_location(
        subscriber_location_weights=location_visits,
        weight_column="value",
        include_unlocatable=False,
    )
    ml_true = majority_location(
        subscriber_location_weights=location_visits,
        weight_column="value",
        include_unlocatable=True,
    )
    assert ml_false.query_id in set(dep.query_id for dep in ml_true.dependencies)


@pytest.mark.parametrize("min_weight", [2, 2.5])
def test_minimum_total_weight(min_weight, get_dataframe):
    weights = CustomQuery(
        """
        SELECT 'DUMMY_SUBSCRIBER' AS subscriber,
               'DUMMY_LOCATION' AS location_id,
               1 AS value
        """,
        column_names=["subscriber", "location_id", "value"],
    )
    weights.spatial_unit = CellSpatialUnit()
    ml_without_min = majority_location(
        subscriber_location_weights=weights,
        weight_column="value",
    )
    ml_with_min = majority_location(
        subscriber_location_weights=weights,
        weight_column="value",
        minimum_total_weight=min_weight,
        include_unlocatable=False,
    )
    ml_with_min_unlocatable = majority_location(
        subscriber_location_weights=weights,
        weight_column="value",
        minimum_total_weight=min_weight,
        include_unlocatable=True,
    )
    ml_without_min_result = get_dataframe(ml_without_min)
    ml_with_min_result = get_dataframe(ml_with_min)
    ml_with_min_unlocatable_result = get_dataframe(ml_with_min_unlocatable)
    # Check that without minimum weight, the query returns expected result
    assert_frame_equal(
        ml_without_min_result,
        df.from_records(
            [("DUMMY_SUBSCRIBER", "DUMMY_LOCATION")],
            columns=["subscriber", "location_id"],
        ),
    )
    # Check that with minimum weight, subscriber is not locatable
    assert len(ml_with_min_result) == 0
    # Check that unlocatable subscriber is returned if requested
    assert_frame_equal(
        ml_with_min_unlocatable_result,
        df.from_records(
            [("DUMMY_SUBSCRIBER", None)],
            columns=["subscriber", "location_id"],
        ),
    )


def test_negative_weights_ignored(get_dataframe):
    """
    Test that rows with negative weight in the subscriber location weights query
    are ignored when calculating majority location.
    """
    # If negative weights were included, all three locations here would be "majority locations"
    # because they all have weight > -3/2
    weights = CustomQuery(
        """
        SELECT 'DUMMY_SUBSCRIBER' AS subscriber,
               'DUMMY_LOCATION_' || generate_series(1, 3)::text AS location_id,
               -1 AS value
        """,
        column_names=["subscriber", "location_id", "value"],
    )
    weights.spatial_unit = CellSpatialUnit()
    ml_without_unlocatable = majority_location(
        subscriber_location_weights=weights,
        weight_column="value",
        include_unlocatable=False,
    )
    ml_with_unlocatable = majority_location(
        subscriber_location_weights=weights,
        weight_column="value",
        include_unlocatable=True,
    )
    ml_without_unlocatable_result = get_dataframe(ml_without_unlocatable)
    ml_with_unlocatable_result = get_dataframe(ml_with_unlocatable)
    # Check that subscriber is not locatable
    assert len(ml_without_unlocatable_result) == 0
    # Check that subscriber with only negative weights _is_ included as unlocatable if requested
    assert_frame_equal(
        ml_with_unlocatable_result,
        df.from_records(
            [("DUMMY_SUBSCRIBER", None)],
            columns=["subscriber", "location_id"],
        ),
    )


def test_weight_column_missing(location_visits):
    lv = location_visits
    with pytest.raises(ValueError):
        ml = majority_location(
            subscriber_location_weights=lv, weight_column="nonexistant"
        )


def test_subscriber_missing():
    with pytest.raises(ValueError):
        majority_location(
            subscriber_location_weights=TotalLocationEvents("2016-01-01", "2016-01-02"),
            weight_column="value",
        )


def test_spatial_unit_missing():
    with pytest.raises(ValueError):
        majority_location(
            subscriber_location_weights=HandsetStats(
                "width", "count", subscriber_handsets=None
            ),
            weight_column="value",
        )


def test_negative_minimum_weight(location_visits):
    with pytest.raises(ValueError, match="minimum_total_weight cannot be negative"):
        majority_location(
            subscriber_location_weights=location_visits,
            weight_column="value",
            minimum_total_weight=-1,
        )


def test_spatial_units(exemplar_spatial_unit_param, get_dataframe):
    lv = LocationVisits(
        DayTrajectories(
            daily_location("2016-01-01", spatial_unit=exemplar_spatial_unit_param),
            daily_location("2016-01-02", spatial_unit=exemplar_spatial_unit_param),
        )
    )
    ml = majority_location(subscriber_location_weights=lv, weight_column="value")
    assert ml.column_names == (["subscriber"] + lv.spatial_unit.location_id_columns)
    out = get_dataframe(ml)
    assert len(out) >= 0
