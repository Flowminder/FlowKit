# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Spatial utility class that creates a regular grid over the country
of interest. This can later be used as a spatial level in flowmachine.
"""
from typing import List

from flowmachine.core.mixins import GeoDataMixin

from .query import Query


class Grid(GeoDataMixin, Query):
    """
    Creates a set of polygons representing a regular grid
    over the given polygon, which is by default the whole of
    the country you are working with.

    Parameters
    ----------
    size : float or int
        Side length of the squares of the grid in km.
    geom : str, default 'geography.admin0'
        A string pointing to the geometry object that you
        want to put a grid on. (The whole of you country by default)
    """

    def __init__(self, size, geom="geography.admin0"):
        """"""

        self.geom = geom
        self.size = size

        super().__init__()

    def _geo_augmented_query(self):
        """
        Returns a version of this query with geom and gid columns.

        Returns
        -------
        str
            A version of this query with geom and gid columns
        """

        return (
            f"SELECT grid_id, geom_square as geom, row_number() OVER (ORDER BY longitude, latitude) as gid FROM ({self.get_query()}) as x",
            ["grid_id", "geom", "gid"],
        )

    @property
    def column_names(self) -> List[str]:
        return ["grid_id", "geom_square", "geom_point", "longitude", "latitude"]

    def _make_query(self):

        grid_sql = f"""
        SELECT
            geom AS geom_square,
            ST_Centroid(geom) AS geom_point,
            ST_X(ST_Centroid(geom)) AS longitude,
            ST_Y(ST_Centroid(geom)) AS latitude
        FROM
            (SELECT (
                ST_Dump(makegrid_2d( (SELECT geom FROM {self.geom}),
                 {self.size * 1000}) -- cell step in meters
             )).geom AS geom)
             AS q_grid
        """

        sql = f"""
        SELECT
            '{str(self.size).replace(".", "_")}' || '_' || row_number() OVER (ORDER BY longitude, latitude) AS grid_id,
            geom_square,
            geom_point,
            longitude,
            latitude
        FROM
            ({grid_sql}) AS grid
        """
        return sql
