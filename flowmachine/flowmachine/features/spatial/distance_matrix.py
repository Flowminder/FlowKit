# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Utility methods for calculating a distance
matrix from a given point collection.

"""
from typing import List

from flowmachine.utils import get_columns_for_level
from ...core.query import Query
from ...core.mixins import GraphMixin


class DistanceMatrix(GraphMixin, Query):
    """
    Calculates the complete distance matrix between a 
    location set. This is useful for the further 
    computation of distance travelled, area of influence, 
    and other features.

    Distance is returned in Km.

    Parameters
    ----------
    locations_table : str
        Locations table where to find the locations to compute
        distances for.

    id_column : str
        The column with the unique ID for each location.
        The default parameter is 'id'.

    geom_column : str
        Geometry column for calculating distances.
        The default is 'geom_point'.

    date : str
        Date string in ISO format (e.g. '2016-01-22')
        for retrieving a VersionedInfrastructure()
        object. If nothing is passed, that object
        will be instantiated with the current date.

    return_geometry : bool
        If True, geometries are returned in query
        (represented as WKB in a dataframe). This
        is an useful option if one is computing
        other geographic properties out of the

    Examples
    --------
    >>> DistanceMatrix().get_dataframe()
        origin   destination   distance
    0   8wPojr   GN2k0G        789.232397
    1   GN2k0G   8wPojr        789.232397
    2   8wPojr   j1m77j        786.102789
    3   j1m77j   8wPojr        786.102789
    4   DbWg4K   8wPojr        757.977718

    """

    def __init__(self, level="versioned-cell", date=None, return_geometry=False):

        if level not in {"versioned-site", "versioned-cell"}:
            raise ValueError("Only point locations are supported at this time.")
        self.level = level
        self.date = date
        self.return_geometry = return_geometry

        super().__init__()

    @property
    def column_names(self) -> List[str]:
        cols = get_columns_for_level(self.level)

        cols.remove("lat")
        cols.remove("lon")

        col_names = [f"{c}_from" for c in cols]
        col_names += [f"{c}_to" for c in cols]
        col_names += [f"{c}_from" for c in ("lon", "lat")]
        col_names += [f"{c}_to" for c in ("lon", "lat")]
        col_names += ["distance"]
        if self.return_geometry:
            col_names += ["geom_origin", "geom_destination"]
        return col_names

    def _make_query(self):
        cols = get_columns_for_level(self.level)
        sql_location_table = "SELECT * FROM infrastructure." + (
            "sites" if self.level == "versioned-site" else "cells"
        )
        cols.remove("lat")
        cols.remove("lon")

        from_cols = ", ".join(
            "A.{c_id_safe} AS {c}_from".format(
                c_id_safe="id" if c.endswith("id") else c, c=c
            )
            for c in cols
        )
        to_cols = ", ".join(
            "B.{c_id_safe} AS {c}_to".format(
                c_id_safe="id" if c.endswith("id") else c, c=c
            )
            for c in cols
        )

        return_geometry_statement = ""
        if self.return_geometry:
            return_geometry_statement = """
                ,
                A.geom_point AS geom_origin,
                B.geom_point AS geom_destination
            """

        sql = """

            SELECT
                {froms},
                {tos},
                ST_X(A.geom_point::geometry) AS lon_from,
                ST_Y(A.geom_point::geometry) AS lat_from,
                ST_X(B.geom_point::geometry) AS lon_to,
                ST_Y(B.geom_point::geometry) AS lat_to,
                ST_Distance(
                    A.geom_point::geography, 
                    B.geom_point::geography
                ) / 1000 AS distance
                {return_geometry_statement}
            FROM ({location_table_statement}) AS A
            CROSS JOIN ({location_table_statement}) AS B
            ORDER BY distance DESC
            
        """.format(
            location_table_statement=sql_location_table,
            froms=from_cols,
            tos=to_cols,
            return_geometry_statement=return_geometry_statement,
        )

        return sql
