from typing import List
from flowmachine.core.query import Query


class MobilityEstimation(Query):
    def __init__(
        self,
        start,
        stop,
        agg_unit,
    ):
        pass

    @property
    def column_names(self) -> List[str]:
        return [
            "month",
            "resident_count",
            "resident_count_change",
            "resident_count_change_percent",
            "resident_count_change_median",
            "resident_count_change_median_percent",
            "high_mobility_resident_percent",
            "displaced_residents_percent",
        ]

    def _make_query(self):
        return f"""
        SELECT 
        1 as month,
        2 AS resident_count,
        3 AS resident_count_change,
        4 AS resident_count_change_percent,
        5 AS resident_count_change_median
        6 AS resident_count_change_median_percent,
        7 AS high_mobility_resident_percent,
        8 AS displaced_residents_percent
        """
