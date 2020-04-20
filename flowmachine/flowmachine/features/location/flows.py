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

from ...core.query import Query
from ...core.mixins import GeoDataMixin, GraphMixin
from ...core.errors import InvalidSpatialUnitError

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
        loc_cols_from_string = ",".join([f"{col}_from" for col in loc_cols])
        loc_cols_to_string = ",".join([f"{col}_to" for col in loc_cols])
        loc_cols_from_aliased_string = ",".join(
            [f"{col}_from AS {col}" for col in loc_cols]
        )
        loc_cols_to_aliased_string = ",".join(
            [f"{col}_to AS {col}" for col in loc_cols]
        )

        agg_qry = f"""
                WITH flows AS ({self.get_query()})
                SELECT
                    {loc_cols_string},
                    json_strip_nulls(outflows) as outflows,
                    json_strip_nulls(inflows) as inflows
                FROM
                (
                    SELECT
                        {loc_cols_from_aliased_string},
                        json_object_agg({loc_cols[0]}_to, value) AS outflows
                    FROM flows
                    GROUP BY {loc_cols_from_string}
                ) x
                FULL JOIN
                (
                    SELECT
                        {loc_cols_to_aliased_string},
                        json_object_agg({loc_cols[0]}_from, value) AS inflows
                    FROM flows
                    GROUP BY {loc_cols_to_string}
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
    """

    def __init__(self, loc1, loc2):
        if loc1.spatial_unit != loc2.spatial_unit:
            raise InvalidSpatialUnitError(
                "You cannot compute flows for locations on different spatial units"
            )

        self.spatial_unit = loc1.spatial_unit
        self.joined = loc1.join(
            loc2, on_left="subscriber", left_append="_from", right_append="_to"
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
        self.loc_from = ",".join([c for c in cols if c.endswith("_from")])
        self.loc_to = ",".join([c for c in cols if c.endswith("_to")])
        self.spatial_unit = flow.spatial_unit
        super().__init__()

    # Returns a query that groups by one column and sums the count
    def _groupby_col(self, sql_in, col):

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
        return [f"{col}_to" for col in cols] + ["value"]
