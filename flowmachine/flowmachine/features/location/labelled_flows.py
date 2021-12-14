# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-

from typing import List

from flowmachine.core import Query
from flowmachine.core.mixins import GeoDataMixin
from flowmachine.features.location.flows import (
    FlowLike,
)


class LabelledFlows(FlowLike, GeoDataMixin, Query):
    """
    An object representing movement of subscribers between both locations and labelled categories within those
    locations.


    Parameters
    ----------
    loc1 : Query
        Object representing the locations of people within the
        first time frame of interest
    loc2 : Query
        As above for the second period
    labels : Query
        A query returning a set of unique 'subscriber' columns and at least one categorical column.
    label_columns : List[str] default ('value',)
        Columns in `labels` query to disaggregate on. Will be present in output query as "{column_name}_label"
    join_type : {"inner", "full outer", "left", "right", "left outer", "right outer"} default "inner"
        Join type of the join between loc_1 and loc_2

    Properties
    ----------
    out_label_columns : List[str]
        The names of the label columns with suffix attached

    Examples
    --------

    Get count of subscribers, per handset type, who moved between locations between the 1st and the 2nd Jan, 2016.
    Please note that SubscriberHandsetCharacteristic is *not* exposed to this query through the API due to
    deanonymisation risk - it is used here as an illustrative example.

    >>>     loc_1 = locate_subscribers(
    ...        "2016-01-01",
    ...        "2016-01-02",
    ...        spatial_unit=make_spatial_unit("admin", level=3),
    ...        method="most-common",
    ...    )
    ...
    ...    loc_2 = locate_subscribers(
    ...        "2016-01-02",
    ...        "2016-01-03",
    ...        spatial_unit=make_spatial_unit("admin", level=3),
    ...        method="most-common",
    ...    )
    ...
    ...    labels_1 = SubscriberHandsetCharacteristic(
    ...        "2016-01-01", "2016-01-03", characteristic="hnd_type"
    ...    )
    ...
    ...    LabelledFlows(loc1=loc_1, loc2=loc_2, labels=labels_1)

           pcod_from      pcod_to value_label  value
    0    524 1 01 04  524 1 02 09       Smart      1
    1    524 1 01 04  524 1 03 13     Feature      1
    2    524 1 01 04  524 2 04 20     Feature      1
    3    524 1 01 04  524 2 05 24     Feature      2
    4    524 1 01 04  524 2 05 29       Smart      1
    ..           ...          ...         ...    ...

    """

    def __init__(
        self,
        *,
        loc1: Query,
        loc2: Query,
        labels: Query,
        label_columns: List[str] = ("value",),
        join_type: str = "inner",
    ):

        if loc1.spatial_unit != loc2.spatial_unit:
            raise ValueError("Flows must have the same spatial unit")
        for label_column in label_columns:
            if label_column not in labels.column_names:
                raise ValueError(f"{label_column} not present in {labels.column_names}")
        if "subscriber" in label_columns:
            raise ValueError("'subscriber' cannot be a label column")

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
            self.labels, on_left="subscriber", right_append="_label", how="left"
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

    @property
    def out_spatial_columns(self) -> List[str]:
        return [f"{col}_from" for col in self.spatial_unit.location_id_columns] + [
            f"{col}_to" for col in self.spatial_unit.location_id_columns
        ]

    def _make_query(self):

        group_cols = ",".join(self.out_spatial_columns + self.out_label_columns)

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
