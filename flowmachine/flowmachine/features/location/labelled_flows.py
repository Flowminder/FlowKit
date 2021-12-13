from typing import List

from flowmachine.core import Query
from flowmachine.core.mixins import GeoDataMixin
from flowmachine.features.location.flows import (
    FlowLike,
    Flows,
    BaseInOutFlow,
    OutFlow,
    InFlow,
)


class LabelledFlows(FlowLike, GeoDataMixin, Query):
    def __init__(
        self, *, loc1, loc2, labels, label_columns=("value",), join_type="inner"
    ):
        """
        Creates a
        Parameters
        ----------
        loc1
        loc2
        labels
        label_columns
        """

        # Check spatial units are same
        # Check label columns are
        if loc1.spatial_unit != loc2.spatial_unit:
            raise ValueError("Flows must have the same spatial unit")
        for label_column in label_columns:
            if label_column not in labels.column_names:
                raise ValueError(f"{label_column} not present in {labels.column_names}")

        self.loc_from = loc1
        self.loc_to = loc2
        self.labels = labels
        self.label_columns = list(label_columns)
        self.spatial_unit = loc1.spatial_unit

        loc_joined = loc1.join(
            loc2,
            on_left="subscriber",
            left_append="_from",
            right_append="_to",
            how=join_type,
        )

        self.joined = loc_joined.join(
            self.labels, on_left="subscriber", right_append="_label"
        )

        super().__init__()

    @property
    def column_names(self) -> List[str]:
        loc_cols = self.spatial_unit.location_id_columns
        return (
            [f"{col}_from" for col in loc_cols]
            + [f"{col}_to" for col in loc_cols]
            + self.out_label_columns
            + ["value"]
        )

    @property
    def out_label_columns(self) -> List[str]:
        return [f"{col}_label" for col in self.label_columns]

    def _make_query(self):

        group_cols = ",".join(self.joined.column_names[1:])

        grouped = f"""
        SELECT
            {group_cols},
            count(*) as value
        FROM 
            ({self.joined.get_query()}) AS joined
        GROUP BY
            {group_cols}
        ORDER BY {group_cols} DESC
        """

        return grouped
