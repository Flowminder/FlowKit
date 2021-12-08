from typing import List

from flowmachine.core import Query
from flowmachine.core.mixins import GeoDataMixin
from flowmachine.features.location.spatial_aggregate import SpatialAggregate


class LabelledSpatialAggregate(GeoDataMixin, Query):
    """
    Class representing a disaggregation of a SpatialAggregate by some label or set of labels
    Returns a query with
    """

    def __init__(
        self,
        locations: Query,
        subscriber_labels: Query,
        label_columns: List[str] = ("value",),
    ):
        self.locations = locations
        self.subscriber_labels = subscriber_labels
        self.label_columns = list(label_columns)
        self.spatial_unit = locations.spatial_unit
        self.label_columns = label_columns
        self.out_label_columns = [
            f"label_{label_col}" for label_col in self.label_columns
        ]
        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return (
            list(self.spatial_unit.location_id_columns)
            + [f"label_{label}" for label in self.label_columns]
            + ["value"]
        )

    @property
    def out_label_columns_as_string_list(self):
        return ",".join(self.out_label_columns)

    def _make_query(self):

        aggregate_cols = ",".join(
            f"agg.{agg_col}" for agg_col in self.spatial_unit.location_id_columns
        )
        label_select = ",".join(
            f"labels.{label_col} AS {out_label_col}"
            for label_col, out_label_col in zip(
                self.label_columns, self.out_label_columns
            )
        )
        label_group = ",".join(
            f"labels.{label_col}" for label_col in self.label_columns
        )

        sql = f"""
            SELECT
                {aggregate_cols}, {label_select}, count(*) AS value
            FROM 
                ({self.locations.get_query()}) AS agg
            JOIN
                ({self.subscriber_labels.get_query()}) AS labels USING (subscriber)
            GROUP BY
                {aggregate_cols}, {label_group}
            """
        return sql
