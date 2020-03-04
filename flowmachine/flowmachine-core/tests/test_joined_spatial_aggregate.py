# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import warnings
from typing import List
from unittest.mock import Mock

import pytest

from flowmachine_core.query_bases.spatial_unit import make_spatial_unit
from flowmachine_core.query_bases.query import Query
from utility_queries.joined_spatial_aggregate import JoinedSpatialAggregate


class Metric(Query):
    @property
    def column_names(self) -> List[str]:
        return ["subscriber", "value"]

    def _make_query(self):
        return """
        SELECT * FROM (VALUES ('a', 1), ('b', 2), ('c', 3), ('d', 1)) as t(subscriber, value)
        """


class CategorialMetric(Query):
    @property
    def column_names(self) -> List[str]:
        return ["subscriber", "value"]

    def _make_query(self):
        return """
        SELECT * FROM (VALUES ('a', 'a'), ('b', 'a'), ('c', 'b'), ('d', 'e')) as t(subscriber, value)
        """


class Location(Query):
    spatial_unit = Mock(location_id_columns=["pcod"])

    @property
    def column_names(self) -> List[str]:
        return ["subscriber", "pcod"]

    def _make_query(self):
        return """
            SELECT * FROM (VALUES ('a', 'a'), ('b', 'a'), ('c', 'b'), ('d', 'a')) as t(subscriber, pcod)
            """


@pytest.mark.parametrize(
    "method, expected", [("avg", 4 / 3), ("mode", 1), ("median", 1)]
)
def test_joined_aggregate(method, expected, get_dataframe):
    """
    Test join aggregate.
    """
    joined = JoinedSpatialAggregate(
        metric=Metric(), locations=Location(), method=method
    )
    assert (
        pytest.approx(expected)
        == get_dataframe(joined).set_index("pcod").loc["a"].value
    )


def test_joined_agg_date_mismatch():
    """
    Test that join aggregate with mismatched dates raises a warning.
    """

    with pytest.warns(UserWarning):
        JoinedSpatialAggregate(
            metric=Mock(start="2016-01-01", stop="2016-01-01"),
            locations=Mock(start="2016-01-02", stop="2016-01-01"),
        )

    with pytest.warns(UserWarning):
        JoinedSpatialAggregate(
            metric=Mock(start="2016-01-01", stop="2016-01-01"),
            locations=Mock(start="2016-01-01", stop="2016-01-02"),
        )


def test_joined_agg_hours_mismatch():
    """
    Test that join aggregate with mismatched hours doesn't warn.
    """
    with warnings.catch_warnings(record=True) as w:
        JoinedSpatialAggregate(
            metric=Mock(start="2016-01-01 18:00", stop="2016-01-01"),
            locations=Mock(start="2016-01-01", stop="2016-01-01"),
        )
        assert not w


def test_can_be_aggregated_admin3_distribution(get_dataframe):
    """
    Categorical queries can be aggregated to a spatial level with 'distribution' method.
    """

    agg = JoinedSpatialAggregate(
        metric=CategorialMetric(), locations=Location(), method="distr"
    )
    df = get_dataframe(agg)
    assert ["pcod", "metric", "key", "value"] == list(df.columns)
    assert all(df[df.metric == "value"].groupby("pcod").sum() == 1.0)
