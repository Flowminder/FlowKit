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


class LabelledFlowLike(FlowLike):
    def outflow(self):
        return LabelledOutFlow(self)

    def inflow(self):
        return LabelledInFlow(self)


class LabelledBaseInOutFlow(BaseInOutFlow):
    def __init__(self, flow):
        cols = self.flow.column_names
        self.label = ",".join(c for c in cols if c.endswith("_label"))
        super().__init__(flow)

    def _groupby_col(self, sql_in, col):
        labelled_col = ",".join([col, self.label])
        return super()._groupby_col(sql_in, labelled_col)


class LabelledOutFlow(OutFlow, LabelledBaseInOutFlow):
    @property
    def column_names(self) -> List[str]:
        return super().column_names + self.flow.out_label_columns


class LabelledInFlow(InFlow, LabelledBaseInOutFlow):
    @property
    def column_names(self) -> List[str]:
        return super().column_names + self.flow.out_label_columns


class LabelledFlows(LabelledFlowLike, GeoDataMixin, Query):
    def __init__(
        self, *, loc_from, loc_to, labels, label_columns=("value",), join_type="inner"
    ):
        """
        Creates a
        Parameters
        ----------
        loc_from
        loc_to
        labels
        label_columns
        """

        # Check spatial units are same
        # Check label columns are

        self.loc_from = loc_from
        self.loc_to = loc_to
        self.labels = labels
        self.label_columns = list(label_columns)
        self.spatial_unit = loc_from.spatial_unit

        self.out_label_columns = [f"{col}_label" for col in label_columns]

        loc_joined = loc_from.join(
            loc_to,
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
