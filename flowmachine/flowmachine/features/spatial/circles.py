# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""

Classes for creating raster sums over circles in space. Will be used 
to implement Radiation model of mobility.



"""
from typing import List

from ...core import Query, CustomQuery
from ...core.mixins import GeoDataMixin
from ..raster import RasterStatistics


class Circle:
    """
    This is a simple class for storing information about the location and
    extent of a circle - this could represent the extent of a city. It is to be used in conjuction 
    with the Circle class in order to for a set of geometries that represent the circles in question.
    
    Parameters
    ----------
    lon, lat : int
        The longitude and latitude of the circle centre
    radius : float
        The radius in meters to use as the extent of the circle
    names : str
        An ID string for the circle
    
    Examples
    --------
    >>> c = Circle(2, 3, 4, 'bob')
    
    >>> c
    Circle(2, 3, 4,'bob')
    """

    def __init__(self, lon, lat, radius, name):
        self.lon, self.lat = lon, lat
        self.point_sql = (
            f"ST_GeomFromText('POINT ({self.lon} {self.lat})',4326)::geography"
        )
        self.name = name
        self.radius = radius

    def __repr__(self):

        return f"Circle(lon={self.lon},lat={self.lat},radius={self.radius},name={self.name})"


class CircleGeometries(GeoDataMixin, Query):
    """
    This class will form the required geometries for a set of circles defined by
    an iterable of Circle objects. This class is used by CircleRasterPops which calculates
    population based on a raster.
    
    Parameters
    ----------
    circles : iterable of Circle
        The circular regions for which geometries are required
        
    Examples
    --------
    >>> cl = Circle(2, 3, 4, 'bob')
    >>> c = CircleGeometries([cl])
    >>> c.head()
    geom     name
    01030000... bob

    >>> c.raster_sum()
    """

    def __init__(self, circles):

        self.names = [c.name for c in circles]
        self.radii = [c.radius for c in circles]
        self.centres = f"[{','.join([c.point_sql for c in circles])}]"

        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return ["geom", "name"]

    def _make_query(self):

        circ_sql = f"""
        select
            *
        from
            unnest(ARRAY{self.names}, ARRAY{self.radii}, ARRAY{self.centres}) AS t(names, radii, centres)
        """

        return f"""
        select 
            ST_buffer(centres,radii) as geom,
            names as name 
        from 
            ({circ_sql}) as _
        """

    def _geo_augmented_query(self):

        sql = f"""
        select
            row_number() over() as gid,
            *
        from
            ({self.get_query()}) as t
        """

        return sql, ["name", "geom", "gid"]

    def raster_sum(self, raster):
        """
        Returns the raster sum over the circle geometries

        Examples
        --------
        >>> lons = [85.3240,83.9956]
        >>> lats = [27.7172,28.2380]
        >>> names = ['Kathmandu','Pokhara']
        >>> radii = [4000,11000]

        >>> circles = [Circle(*vals) for vals in zip(lons,lats,radii,names)]
        >>> cp = CircleGeometries(circles)
        >>> rs = cp.raster_sum('population.small_nepal_raster')
        >>> rs.get_dataframe()

        name     statistic
        Kathmandu  1.135396e+06
        Pokhara  4.052404e+05


        """
        return RasterStatistics(raster, vector=self, grouping_element="name")
