from flowmachine.core.server.query_schemas.mobility_estimation import MobilityEstimationExposed

def test_exposed_mobility_estimation(mocked_connections):
    query = MobilityEstimationExposed(
        start_date="01-02-2021",
        end_date="02-02-2021",
        aggregation_unit="admin_1"
    )
    assert query._flowmachine_query_obj._make_sql("test")[0] == f"""
    'EXPLAIN (ANALYZE TRUE, TIMING FALSE, FORMAT JSON) CREATE TABLE test AS (
        SELECT 
        1 as month,
        2 AS resident_count,
        3 AS resident_count_change,
        4 AS resident_count_change_percent,
        5 AS resident_count_change_median
        6 AS resident_count_change_median_percent,
        7 AS high_mobility_resident_percent,
        8 AS displaced_residents_percent
    )
        """
