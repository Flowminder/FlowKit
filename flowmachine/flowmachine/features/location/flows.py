# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Definition of the flows class, which is the difference in 
locations between two daily or home location classes, 
aggregated to a location level.



"""
import logging
from abc import ABCMeta
from typing import List

from uuid import uuid4

from ...core.query import Query
from ...core.mixins import GeoDataMixin, GraphMixin
from ...utils.utils import get_columns_for_level

logger = logging.getLogger("flowmachine").getChild(__name__)


class BaseFlow(Query, metaclass=ABCMeta):
    """
    Abstract flow class. There are different forms of flows, e.g. scaled flow
    and a relative flow. They all have the same form, i.e. from_location, to_location.
    All of them can be scaled and subtracted.
    """

    def __sub__(self, other):
        return FlowSub(self, other)

    def __add__(self, other):
        return FlowSum(self, other)

    def __radd__(self, other):
        return FlowSum(self, other)

    def __truediv__(self, other):
        return FlowDiv(self, other)

    def __mul__(self, other):
        return FlowMul(self, other)

    def __pow__(self, exponent):
        return FlowPow(self, exponent)

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

    @property
    def index_cols(self):
        cols = get_columns_for_level(self.level, self.column_name)
        return [["{}_from".format(x) for x in cols], ["{}_to".format(x) for x in cols]]

    @property
    def column_names(self) -> List[str]:
        cols = get_columns_for_level(self.level, self.column_name)
        return (
            [f"{col}_from" for col in cols] + [f"{col}_to" for col in cols] + ["count"]
        )


class EdgeList(BaseFlow):
    """
    Takes a 'normal' Query object, and presents it in a Flows-like
    form. Essentially, reindexes it by the cross product of the
    enclosed query's columns. This allows you to use arithmetic
    operations with a Flows, and a regular Query object.

    In vector terms, equivalent to multiplying the query by a transposed one-filled
    version of itself, expressed as an edgelist.

    With `left_handed=False`, this is equivalent to multiplying a one-filled version
    of the query by the transposed query.

    Parameters
    ----------
    query : Query
        Query object to construct an edgelist for, required to have a location column, and at least
        one numeric column
    count_column : str
        Name of the column to make the count column, defaults to the rightmost.
    left_handed : bool
        Handedness of the reindex, set to False for right-handed.

    Examples
    --------

    >>> dl = daily_location("2016-01-01")
    >>> dl.aggregate().get_dataframe()
                 name  total
    0           Rasuwa     11
    1         Sindhuli     14
    2            Gulmi     12
    >>> f = EdgeList(dl.aggregate())
    >>> f.get_dataframe()
        name_from         name_to  count
    0      Rasuwa          Rasuwa     11
    1      Rasuwa        Sindhuli     11
    2      Rasuwa           Gulmi     11
    >>> f = EdgeList(dl.aggregate(), left_handed=False)
    >>> f.get_dataframe()
        name_from         name_to  count
    0      Rasuwa          Rasuwa     11
    1      Rasuwa        Sindhuli     14
    2      Rasuwa           Gulmi     12
    >>> f = EdgeList(dl.aggregate(), left_handed=False, count_column="name")
    >>> f.get_dataframe()
        name_from         name_to  count
    0      Rasuwa          Rasuwa     Rasuwa
    1      Rasuwa        Sindhuli     Sindhuli
    2      Rasuwa           Gulmi     Gulmi
    >>> f = EdgeList(dl.aggregate(), count_column="name")
    >>> f.get_dataframe()
        name_from         name_to  count
    0      Rasuwa          Rasuwa     Rasuwa
    1      Rasuwa        Sindhuli     Rasuwa
    2      Rasuwa           Gulmi     Rasuwa
    """

    def __init__(self, query, count_column=None, left_handed=True):
        self.level = query.level
        self.column_name = query.column_name
        if count_column is None:
            self.count_column = query.column_names[-1]
        else:
            self.count_column = count_column
        if self.count_column not in query.column_names:
            raise ValueError(
                "{} is not a column. Must be one of {}.".format(
                    count_column, query.column_names
                )
            )
        self.wrapped_query = query
        self.primary = "left" if left_handed else "right"
        super().__init__()

    def _make_query(self):
        cols = get_columns_for_level(self.level, self.column_name)
        left_cols = ",".join(
            "locs_left.{col} as {col}_from".format(col=col) for col in cols
        )
        right_cols = ",".join(
            "locs_right.{col} as {col}_to".format(col=col) for col in cols
        )
        qur = """
        WITH locs AS ({locs})
            SELECT {l_cols},
                {r_cols},
                locs_{primary}.{measure_col} as count
            FROM locs as locs_left CROSS JOIN locs as locs_right
        """.format(
            locs=self.wrapped_query.get_query(),
            l_cols=left_cols,
            r_cols=right_cols,
            measure_col=self.count_column,
            primary=self.primary,
        )
        return qur


class Flows(GeoDataMixin, GraphMixin, BaseFlow):
    """
    An object representing the difference in locations between two location
    type objects.

    Parameters
    ----------
    loc1 : daily_location, or HomeLocation object
        Object representing the locations of people within the
        first time frame of interest
    loc2 : daily_location, or HomeLocation object
        As above for the second period
    """

    def __init__(self, loc1, loc2):
        """

        """

        if loc1.level != loc2.level:
            raise ValueError(
                "You cannot compute flows for locations on " + "different levels"
            )

        self.level = loc1.level
        self.column_name = loc1.column_name
        self.joined = loc1.join(
            loc2, on_left="subscriber", left_append="_from", right_append="_to"
        )
        logger.info(
            "{} locations are pre-calculated.".format(loc1.is_stored + loc2.is_stored)
        )
        super().__init__()

    def _make_query(self):
        group_cols = ",".join(self.joined.column_names[1:])

        grouped = """
        SELECT
            {group_cols},
            count(*)
        FROM 
            ({joined}) AS joined
        GROUP BY
            {group_cols}
        ORDER BY {group_cols} DESC
        """.format(
            group_cols=group_cols, joined=self.joined.get_query()
        )

        return grouped

    def _geo_augmented_query(self):
        """
        Returns one of each geom for non-point levels, with the
        flows in/out as properties.

        Returns
        -------
        str
            A version of this query with geom and gid columns
        """
        loc_join = self._get_location_join()
        level = loc_join.level
        if level in ["lat-lon", "versioned-site"]:
            return super()._geo_augmented_query()
        else:
            mapping = loc_join.right_query.mapping
            col_name = mapping.column_name[0]
            l_col_name = (
                "name"
                if ("admin" in level) and (self.column_name is None)
                else col_name
            )
            geom_col = mapping.geom_col
            poly_query = mapping.polygon_table
            if isinstance(poly_query, Query):  # Deal with grids
                poly_query = poly_query.get_query()
            else:
                poly_query = "SELECT * FROM {}".format(poly_query)

            agg_qry = """
            WITH flows AS ({query})
            select {col_name}, json_strip_nulls(outflows) as outflows, json_strip_nulls(inflows) as inflows FROM
            (SELECT {col_name}_from as {col_name}, json_object_agg({col_name}_to, count) AS outflows
            FROM flows
            GROUP BY {col_name}_from
            ) x
            FULL JOIN
            (SELECT {col_name}_to as {col_name}, json_object_agg({col_name}_from, count) AS inflows
            FROM flows
            GROUP BY {col_name}_to
            ) y
            USING ({col_name})
            """.format(
                query=self.get_query(), col_name=l_col_name
            )

            joined_query = """
                                SELECT row_number() over() as gid, {geom_col} as geom, u.*
                                 FROM ({qur}) u
                                  LEFT JOIN
                                 ({poly_query}) g
                                 ON u.{l_col_name}=g.{r_col_name}
                                """.format(
                qur=agg_qry,
                poly_query=poly_query,
                geom_col=geom_col,
                l_col_name=l_col_name,
                r_col_name=col_name,
            )
        return joined_query, [l_col_name, "outflows", "inflows", "geom", "gid"]


class FlowMath(BaseFlow, metaclass=ABCMeta):
    """
    Class representing a maths op with one or more flows.

    Parameters
    ----------
    objA : Flows or numeric
        Flow or scalar to operate on
    objB : Flows or numeric
        Flow or scalar to operate on

    Notes
    -----
    At least one of objA or objB must be a flow
    """

    def __init__(self, objA, objB):
        """


        """

        err_str = "Unsupported operand types {}: {} and {}"
        self.flows = []
        self.numeric = None

        if isinstance(objA, BaseFlow):
            self.level = objA.level
            self.column_name = objA.column_name
            self.flows.append(objA)
        else:
            self.numeric = objA

        if isinstance(objB, BaseFlow):
            self.flows.append(objB)
            self.level = objB.level
            self.column_name = objA.column_name
        else:
            self.numeric = objB

        if not bool(self.flows):
            raise TypeError(err_str.format(self._get_op(), type(objA), type(objB)))
        elif self.numeric and not isinstance(self.numeric, (int, float)):
            raise TypeError(err_str.format(self._get_op(), type(objA), type(objB)))

        if (len(self.flows) > 1) and (self.flows[0].level != self.flows[1].level):
            raise ValueError("You cannot operate on flows of " + "different levels")
        logger.info(
            "{}/{} Flows are pre-calculated.".format(
                sum(1 for fl in self.flows if fl.is_stored), len(self.flows)
            )
        )
        super().__init__()

    def _get_op(self):
        raise NotImplementedError

    def _get_op_name(self):
        raise NotImplementedError

    def _prep_query(self, flowOp, scalarOp):
        """

        Parameters
        ----------
        flowOp : str
            Op to apply to flows named flowA and flowB
        scalarOp : str
            Op to apply to scalars, format string expecting a 'numeric' key.

        Returns
        -------
        str
            Query string with the ops filled in.
        """

        if len(self.flows) > 1:
            cols = ",".join("flowA.{c} AS {c}".format(c=c) for c in self._get_cols())
            join_cols = " AND ".join(
                "flowA.{c}=flowB.{c}".format(c=c) for c in self._get_cols()
            )
            query = """
                    SELECT
                        {cols}, 
                        {flowOp} AS count
                    FROM
                        ({flowA}) AS flowA
                    INNER JOIN
                        ({flowB}) AS flowB
                    ON
                        {join_cols}
                    """.format(
                flowA=self.flows[0].get_query(),
                flowB=self.flows[1].get_query(),
                flowOp=flowOp,
                cols=cols,
                join_cols=join_cols,
            )
        else:
            query = """
                    SELECT
                        {cols}, 
                        {scalarOp} AS count
                    FROM
                        ({flowA}) AS flowA
                    """.format(
                flowA=self.flows[0].get_query(),
                scalarOp=scalarOp.format(numeric=self.numeric),
                cols=",".join(self._get_cols()),
            )
        query += " ORDER BY {cols} DESC".format(cols=",".join(self._get_cols()))
        return query

    def _get_cols(self):
        """

        Returns
        -------
        list
            x_from, x_to for all columns that apply to this level.
        """
        return [
            f"{x}_{y}"
            for y in ("from", "to")
            for x in get_columns_for_level(self.level, self.column_name)
        ]


class FlowPow(FlowMath):
    """
    Class representing a flow raised to a power.

    Parameters
    ----------
    flow : Flows
        Flow to raise element wise to a power
    exponent : Flows or numeric
        Power to raise to

    Notes
    -----

    If a flow is provided, this is performed element wise
    """

    def __init__(self, flow, exponent):
        """

        """
        super().__init__(flow, exponent)

    def _make_query(self):
        return self._prep_query(
            "pow(flowA.count, flowB.count)", "pow(count, {numeric})"
        )

    def _get_op(self):
        return "**"

    def _get_op_name(self):
        return "pow"


class FlowDiv(FlowMath):
    """
    An object representing a flow divided by another
    flow, or a scalar.


    Parameters
    ----------
    flowA : Flows
        Flow to divide.
    divisor : Flows or numeric
        Divisor to apply to flowA

    Notes
    -----
    Division by zero results in a Null value.
    """

    def __init__(self, flowA, divisor):
        """

        """

        super().__init__(flowA, divisor)

    def _make_query(self):
        return self._prep_query(
            "(flowA.count / NULLIF(flowB.count::float, 0))",
            "count/NULLIF({numeric}::float, 0)",
        )

    def _get_op(self):
        return "/"

    def _get_op_name(self):
        return "div"


class FlowMul(FlowMath):
    """
    An object representing a multiplied flow.

    Parameters
    ----------
    flowA : Flows
        Left hand side of multiplication
    multiplier : Flows or numeric
        Multiplier to apply to flowA

    Notes
    -----
    If using a Flow as the multiplier, multiplication is performed
    element wise.

    """

    def __init__(self, flowA, multiplier):
        super().__init__(flowA, multiplier)

    def _make_query(self):
        return self._prep_query("flowA.count * flowB.count", "count*{numeric}")

    def _get_op(self):
        return "*"

    def _get_op_name(self):
        return "mul"


class FlowSum(FlowMath):
    """
    An object representing the sum of two flows.

    Parameters
    ----------
    objA : Flows or numeric
        Object to add to
    objB : Flows or numeric
        Object to add
    """

    def __init__(self, objA, objB):
        super().__init__(objA, objB)

    def _make_query(self):
        if len(self.flows) > 1:
            cols = ",".join(
                "COALESCE(flowA.{c}, flowB.{c}) AS {c}".format(c=c)
                for c in self._get_cols()
            )
            join_cols = " AND ".join(
                "flowA.{c}=flowB.{c}".format(c=c) for c in self._get_cols()
            )
            query = """
            SELECT
                {cols}, 
                (COALESCE(flowA.count, 0) {op} COALESCE(flowB.count, 0)) AS count
            FROM
                ({flowA}) AS flowA
            FULL OUTER JOIN
                ({flowB}) AS flowB
            ON
                {join_cols}
            """.format(
                flowA=self.flows[0].get_query(),
                flowB=self.flows[1].get_query(),
                cols=cols,
                join_cols=join_cols,
                op=self._get_op(),
            )
        else:
            query = """
            SELECT
                {cols},
                count {op} {numeric} as count
            FROM
                ({flowA}) AS flowA
            """.format(
                flowA=self.flows[0].get_query(),
                numeric=self.numeric,
                cols=",".join(self._get_cols()),
                op=self._get_op(),
            )
        query += " ORDER BY {cols} DESC".format(cols=",".join(self._get_cols()))
        return query

    def _get_op(self):
        return "+"

    def _get_op_name(self):
        return "add"


class FlowSub(FlowSum):
    """
    An object representing the difference between two flows.
    Flows missing from one object are treated as if zero in
    the other.

    Parameters
    ----------
    objA : Flows or numeric
        Object to subtract from
    objB : Flows or numeric
        Object to subtract
    """

    def __init__(self, objA, objB):
        super().__init__(objA, objB)

    def _get_op(self):
        return "-"

    def _get_op_name(self):
        return "sub"


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
        self.level = flow.level
        self.column_name = flow.column_name
        super().__init__()

    # Returns a query that groups by one column and sums the count
    def _groupby_col(self, sql_in, col):

        sql_out = """
                  SELECT {c}, sum(count) AS total
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
    through the outflows method of a flows class.
    """

    def _make_query(self):

        return self._groupby_col(self.flow.get_query(), self.loc_from)

    @property
    def index_cols(self):
        cols = get_columns_for_level(self.level, self.column_name)
        return [["{}_from".format(x) for x in cols]]

    @property
    def column_names(self) -> List[str]:
        cols = get_columns_for_level(self.level, self.column_name)
        return [f"{col}_from" for col in cols] + ["total"]


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
        cols = get_columns_for_level(self.level, self.column_name)
        return [["{}_to".format(x) for x in cols]]

    @property
    def column_names(self) -> List[str]:
        cols = get_columns_for_level(self.level, self.column_name)
        return [f"{col}_to" for col in cols] + ["total"]
