# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

from flowmachine.features.subscriber.visited_most_days import VisitedMostDays
from flowmachine.core.server.query_schemas.visited_most_days import (
    VisitedMostDaysExposed,
)
from flowmachine.core import make_spatial_unit


def test_visited_most_days_column_names(get_dataframe):
    """Test that column_names property is accurate"""
    vmd = VisitedMostDays(
        start_date="2016-01-01",
        end_date="2016-01-02",
        spatial_unit=make_spatial_unit("admin", level=1),
    )
    assert get_dataframe(vmd).columns.tolist() == vmd.column_names


def test_visited_most_days_sql_correct(mocked_connections):
    """Test that the correct SQL is generated"""
    query = VisitedMostDays(
        start_date="2021-01-01",
        end_date="2021-02-01",
        spatial_unit=make_spatial_unit("admin", level=1),
    )
    assert (
        query._make_sql("test")[0]
        == """EXPLAIN (ANALYZE TRUE, TIMING FALSE, FORMAT JSON) CREATE TABLE test AS 
        (SELECT pcod, value FROM (
        SELECT 
        'ABC_1.2.3' as pcod,
        42 AS value
        ) _)"""
    )


def test_visited_most_days_result_correct(mocked_connections):
    """Test that the correct result is returned depending on the input"""
    query = VisitedMostDays(
        start_date="2021-01-01",
        end_date="2021-02-01",
        spatial_unit=make_spatial_unit("admin", level=1),
    )
    result = next(iter(query))
    assert result == ("ABC_1.2.3", 42)
