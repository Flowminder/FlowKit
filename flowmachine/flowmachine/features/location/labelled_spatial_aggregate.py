# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from typing import List

from flowmachine.core import Query
from flowmachine.core.mixins import GeoDataMixin
from flowmachine.features.location.spatial_aggregate import SpatialAggregate


class LabelledSpatialAggregate(GeoDataMixin, Query):
    """
    Class representing a disaggregation of a SpatialAggregate by some label or set of labels

    Parameters
    ----------
    locations: Query
        Any query with a subscriber and location columns
    labels: Query
        Any query with a subscriber column
    label_columns: List[str]
        A list of columns in labels to aggregate on

    Examples
    --------

    >>> locations = locate_subscribers(
    ...     "2016-01-01",
    ...     "2016-01-02",
    ...     spatial_unit=make_spatial_unit("admin", level=3),
    ...     method="most-common",
    ... )
    >>> metric = SubscriberHandsetCharacteristic(
    ...     "2016-01-01", "2016-01-02", characteristic="hnd_type"
    ... )
    >>> labelled = LabelledSpatialAggregate(locations=locations,labels=metric)
    >>> labelled.get_dataframe()
               pcod label_value  value
     0  524 3 08 44     Feature     36
     1  524 3 08 44       Smart     28
     2  524 4 12 62     Feature     44
     3  524 4 12 62       Smart     19'

    """

    def __init__(
        self, locations: Query, labels: Query, label_columns: List[str] = ("value",)
    ):
        for label_column in label_columns:
            if label_column not in labels.column_names:
                raise ValueError(f"{label_column} not a column of {labels}")
        if "subscriber" not in locations.column_names:
            raise ValueError(f"Locations query must have a subscriber column")
        if not hasattr(locations, "spatial_unit"):
            raise ValueError(f"Locations must have a spatial_unit attribute")
        if "subscriber" in label_columns:
            raise ValueError(f"'subscriber' cannot be a label")

        self.locations = locations
        self.labels = labels
        self.label_columns = list(label_columns)
        self.spatial_unit = locations.spatial_unit
        self.label_columns = label_columns

        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return (
            list(self.spatial_unit.location_id_columns)
            + self.out_label_columns
            + ["value"]
        )

    @property
    def out_label_columns_as_string_list(self):
        """
        Returns the label column heading as a single string
        """
        return ",".join(self.out_label_columns)

    @property
    def out_spatial_columns(self) -> List[str]:
        """
        Returns all spatial-related columns
        """
        return self.spatial_unit.location_id_columns

    @property
    def out_label_columns(self) -> List[str]:
        """
        Returns all label columns
        """
        return [f"{label_col}_label" for label_col in self.label_columns]

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
            LEFT JOIN
                ({self.labels.get_query()}) AS labels USING (subscriber)
            GROUP BY
                {aggregate_cols}, {label_group}
            """
        return sql
