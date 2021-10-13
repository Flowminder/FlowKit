from decimal import Decimal

from flowmachine.core.server.query_schemas.mobility_estimation import (
    MobilityEstimationExposed,
)
from flowmachine.features.subscriber.mobility_estimation import MobilityEstimation


def test_exposed_mobility_estimation(mocked_connections):
    query = MobilityEstimationExposed(
        start_date="2021-01-01", end_date="2021-02-01", aggregation_unit="admin3"
    )
    assert (
        query._flowmachine_query_obj._make_sql("test")[0]
        == f"""
('EXPLAIN (ANALYZE TRUE, TIMING FALSE, FORMAT JSON) CREATE TABLE test AS 

        (SELECT month, resident_count, resident_count_change,
resident_count_change_percent, resident_count_change_median,
resident_count_change_median_percent, high_mobility_resident_percent,
displaced_residents_percent FROM (
        SELECT 
        1 as month,
        2 AS resident_count,
        3 AS resident_count_change,
        4 AS resident_count_change_percent,
        5 AS resident_count_change_median,
        6 AS resident_count_change_median_percent,
        7 AS high_mobility_resident_percent,
        8 AS displaced_residents_percent
        ) _)'
        """
    )


def test_mobility_estimation():
    query = MobilityEstimation(start="2016-01-01", stop="2016-02-01", agg_unit="admin3")
    out = next(iter(query))
    assert out == (
        "NPL.1.1.1_1",
        9999,
        947,
        136,
        Decimal("16.76942046855733662100"),
        -9052,
        Decimal("-1.50243040212107821500"),
        9999,
        9999,
    )
