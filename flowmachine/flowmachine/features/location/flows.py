# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Definition of the flows class, which is the difference in 
locations between two daily or home location classes, 
aggregated to a spatial unit.



"""

from abc import ABCMeta
from typing import List

from flowmachine.core.query import Query
from flowmachine.core.mixins import GeoDataMixin, GraphMixin
from flowmachine.core.errors import InvalidSpatialUnitError

from flowmachine.core.join import Join

import structlog

logger = structlog.get_logger("flowmachine.debug", submodule=__name__)


class FlowLike(GeoDataMixin, GraphMixin):
    def outflow(self):
        """
        Returns
        -------
        OutFlow
            An outflows object. This is the total number of flows that
            originate from one locations, regardless of their destination.
        """

        return OutFlow(self)

    def inflow(self):
        """
        Returns
        -------
        InFlow
            An inflows object. This is the total number of flows that
            go to one locations, regardless of their origin.
        """

        return InFlow(self)

    def _build_json_agg_clause(self, direction):
        if direction == "in":
            outer_suffix = "to"
            inner_suffix = "from"
        elif direction == "out":
            outer_suffix = "from"
            inner_suffix = "to"
        else:
            raise ValueError(
                f"Expected direction to be 'in' or 'out', not '{direction}'"
            )

        loc_cols = self.spatial_unit.location_id_columns
        if hasattr(self, "out_label_columns"):
            label_cols = self.out_label_columns
        else:
            label_cols = []

        # Key cols are those that will be keys in the nested json object
        key_cols = label_cols + [f"{loc_cols[0]}_{inner_suffix}"]
        loc_cols_aliased_string = ", ".join(
            f"{col}_{outer_suffix} AS {col}" for col in loc_cols
        )

        # Alias column names so we don't have to handle the first nesting differently
        clause = f"""
        SELECT
            {loc_cols_aliased_string},
            {', '.join(key_cols)},
            value AS {direction}flows
        FROM flows
        """

        # Loop through key columns (in reverse order), and add a json_agg layer for each
        for i in range(len(key_cols) - 1, -1, -1):
            group_cols_string = ", ".join(loc_cols)
            for col in key_cols[:i]:
                group_cols_string += f", {col}"
            clause = f"""
            SELECT
                {group_cols_string},
                json_object_agg({key_cols[i]}, {direction}flows) AS {direction}flows
            FROM ({clause}) _
            GROUP BY {group_cols_string}
            """

        return clause

    def _geo_augmented_query(self):
        """
        Returns one of each geom for non-point spatial units, with the
        flows in/out as properties.

        Returns
        -------
        str
            A version of this query with geom and gid columns
        """
        self.spatial_unit.verify_criterion("has_geography")

        loc_cols = self.spatial_unit.location_id_columns
        loc_cols_string = ",".join(loc_cols)

        from_clause = self._build_json_agg_clause("in")
        to_clause = self._build_json_agg_clause("out")

        agg_qry = f"""
                WITH flows AS ({self.get_query()})
                SELECT
                    {loc_cols_string},
                    json_strip_nulls(outflows) as outflows,
                    json_strip_nulls(inflows) as inflows
                FROM
                (
                    {from_clause}
                ) x
                FULL JOIN
                (
                    {to_clause}
                ) y
                USING ({loc_cols_string})
                """

        joined_query = f"""
                SELECT
                    row_number() over() AS gid,
                    *
                FROM ({agg_qry}) AS Q
                LEFT JOIN ({self.spatial_unit.get_geom_query()}) AS G
                USING ({loc_cols_string})
                """

        return joined_query, loc_cols + ["outflows", "inflows", "geom", "gid"]


class Flows(FlowLike, Query):
    """
    An object representing the difference in locations between two location
    type objects.

    Parameters
    ----------
    loc1 : daily_location, or ModalLocation object
        Object representing the locations of people within the
        first time frame of interest
    loc2 : daily_location, or ModalLocation object
        As above for the second period
    join_type : {"inner", "full outer", "left", "right", "left outer", "right outer"} default "inner"
        Join type of the join between loc_1 and loc_2
    """

    def __init__(self, loc1, loc2, join_type="inner"):
        if loc1.spatial_unit != loc2.spatial_unit:
            raise InvalidSpatialUnitError(
                "You cannot compute flows for locations on different spatial units"
            )

        if join_type not in Join.join_kinds:
            raise ValueError(
                f"join_type should be one of {Join.join_kinds}, not {join_type}"
            )

        self.spatial_unit = loc1.spatial_unit
        self.joined = loc1.join(
            loc2,
            on_left="subscriber",
            left_append="_from",
            right_append="_to",
            how=join_type,
        )
        logger.info(
            "{} locations are pre-calculated.".format(loc1.is_stored + loc2.is_stored)
        )
        super().__init__()

    @property
    def index_cols(self):
        cols = self.spatial_unit.location_id_columns
        return [["{}_from".format(x) for x in cols], ["{}_to".format(x) for x in cols]]

    @property
    def column_names(self) -> List[str]:
        cols = self.spatial_unit.location_id_columns
        return (
            [f"{col}_from" for col in cols] + [f"{col}_to" for col in cols] + ["value"]
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


class BaseInOutFlow(GeoDataMixin, Query, metaclass=ABCMeta):
    """
    ABC for both the OutFlow and the Inflow classes.

    Parameters
    ----------
    flow : flowmachine.Flows
        Flows object to derive an in/out flow from
    """

    def __init__(self, flow):
        self.flow = flow
        cols = self.flow.column_names
        # NOTE: Replace with spatial_unit.from_columns
        self.loc_from = ",".join([c for c in cols if c.endswith("_from")])
        self.loc_to = ",".join([c for c in cols if c.endswith("_to")])
        if hasattr(self.flow, "out_label_columns"):
            self.label = ",".join(self.flow.out_label_columns)
        else:
            self.label = None
        self.spatial_unit = flow.spatial_unit
        super().__init__()

    # Returns a query that groups by one column and sums the count
    def _groupby_col(self, sql_in, col):

        if self.label:
            col = ",".join([col, self.label])

        sql_out = """
                  SELECT {c}, sum(value) AS value
                  FROM ({flow}) AS flow
                  GROUP BY {c} ORDER BY {c} DESC
                  """.format(
            flow=sql_in, c=col
        )

        return sql_out


class OutFlow(BaseInOutFlow):
    """
    Class for an outflow. These are the total number of people coming from one
    locations, regardless of where they go to. Note that this is normally initialised
    through the outflows method of a Flows object.
    """

    def _make_query(self):
        return self._groupby_col(self.flow.get_query(), self.loc_from)

    @property
    def index_cols(self):
        cols = self.spatial_unit.location_id_columns
        return [[f"{x}_from" for x in cols]]

    @property
    def column_names(self) -> List[str]:
        cols = self.spatial_unit.location_id_columns
        if hasattr(self.flow, "out_label_columns"):
            return (
                [f"{col}_from" for col in cols]
                + self.flow.out_label_columns
                + ["value"]
            )
        else:
            return [f"{col}_from" for col in cols] + ["value"]


class InFlow(BaseInOutFlow):
    """
    An inflow is the total number of subscribers coming into a region, regardless of where it
    is that they have come from. Normally not instantiated directly, but through a method
    of the Flows class.
    """

    def _make_query(self):

        return self._groupby_col(self.flow.get_query(), self.loc_to)

    @property
    def index_cols(self):
        cols = self.spatial_unit.location_id_columns
        return [[f"{x}_to" for x in cols]]

    @property
    def column_names(self) -> List[str]:
        cols = self.spatial_unit.location_id_columns
        if hasattr(self.flow, "out_label_columns"):
            return (
                [f"{col}_to" for col in cols] + self.flow.out_label_columns + ["value"]
            )
        else:
            return [f"{col}_to" for col in cols] + ["value"]
