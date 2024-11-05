# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Utility method for calculating raster
statistics.
"""
from ...core.query import Query
from typing import List


class RasterStatistics(Query):
    """
    Class for calculating raster statistics.
    Credit for original solution goes to:

        http://movingspatial.blogspot.co.uk/2012/07/postgis-20-intersect-raster-and-polygon.html

    Parameters
    ----------

    raster : str
        Fully qualified table name of raster data.
        This assumes that raster table contains
        a column named `rast`.

    band : int or array
        Band number to use for calculations. Default
        is band 1. For multiple bands, use array: [1, 2]

    vector : str
        Vector layer to use in case of clipping
        and grouping operations. If this option
        is provided, the parameters `vector_property`
        and `grouping_element` need to be provided.
        This can also be a Query object which returns a
        geometry column.

    vector_property : str
        Column name from vector layer with geometry
        data. This is usually called `geom`, but
        can be something else.

    grouping_element : str
        Column name of grouping property to use
        for aggregating values. This property should
        be an equivalent human-readable to the
        values in the `vector_property` column.

    statistic : str
        Type of statistic to calculate from raster.
        Only `sum` is supported at the moment.

    """

    def __init__(
        self,
        raster,
        band=1,
        vector=None,
        vector_property="geom",
        grouping_element=None,
        statistic="sum",
    ):
        if isinstance(band, int):
            band = [band]

        if statistic not in ["sum"]:
            raise NotImplementedError(f"The statistic {statistic} is not implemented.")

        if vector is not None and grouping_element is None:
            raise ValueError("Provide `grouping_element` alongside `vector`.")

        self.raster = raster
        self.band = band
        self.vector = vector
        self.vector_property = vector_property
        self.grouping_element = grouping_element
        self.statistic = statistic

        super().__init__()

    @property
    def column_names(self) -> List[str]:
        if self.vector is None:
            return ["statistic"]
        else:
            return [self.grouping_element, "statistic"]

    def _make_query(self):
        """
        Default query method implemented in the
        metaclass Query().
        """

        if self.vector is None:
            sql = """
            SELECT
                (ST_SummaryStats(W.rast)).sum AS statistic
            FROM
                {raster} as W
            """.format(
                raster=self.raster
            )

        elif self.statistic == "sum":
            # deal with possibility that vector is a query
            try:
                vector_clause = "({})".format(self.vector.get_query())
            except AttributeError:
                raise ValueError("Vector must be a geo-type query")

            sql = """

                SELECT
                    G.{grouping_element},
                    {statistic}((ST_SummaryStats(ST_Clip(R.rast, ARRAY[{band}], G.{vector_property}::geometry))).sum) AS statistic
                FROM
                    {vector_clause} as G,
                    {raster} as R
                WHERE ST_Intersects(G.{vector_property}::geometry, R.rast)
                GROUP BY G.{grouping_element}
                ORDER BY statistic DESC

            """.format(
                grouping_element=self.grouping_element,
                band=",".join([str(_) for _ in self.band]),
                statistic=self.statistic,
                vector_clause=vector_clause,
                vector_property=self.vector_property,
                raster=self.raster,
            )

        return sql
