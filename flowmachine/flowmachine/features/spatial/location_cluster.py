# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Methods for clustering point collections. Methods available
are designed to work with infrastructure elements, but can
be used to any other point collection.



"""
import structlog
from typing import List

from ...core.query import Query
from ...core.mixins import GeoDataMixin

from .versioned_infrastructure import VersionedInfrastructure

logger = structlog.get_logger("flowmachine.debug", submodule=__name__)


class LocationCluster(GeoDataMixin, Query):
    """
    Class for computing clusters of points using different
    algorithms. This class was designed to work with infrastructure
    elements (i.e. towers/sites), but can also be used with other
    point collection as long as that is a table in the 
    database. This class currently implements three methods:
    K-means, DBSCAN, and Area.

    K-means is a clustering algorithm that clusters together points
    based on the point's distance to a point representing the 
    centroid of the cluster. The algorithm has two steps: (a) point
    allocation and (b) centroid re-calculation. In (a) it allocates
    points to the centroid in which they are closest to. In (b) it
    moves the centroid to the mean location of the distances to
    all its members. The process will continue until (b) causes
    the centroid to stop moving, resulting in a Voronoi tesselation.

    For more information, refer to the Wikipedia entry on K-means
    clustering: 

        * https://en.wikipedia.org/wiki/K-means_clustering
    
    The following resource is also very informative:

        * https://www.naftaliharris.com/blog/visualizing-k-means-clustering/

    DBSCAN (Density-Based Spatial Clustering of Applications with Noise)
    is a clustering algorithm that uses the maximum distance between points
    (denoted by ε) as an inclusion criteria to a cluster. Cluster have to
    contain a minimum number of members (denoted by density) to be
    considered valid, otherwise no cluster is assigned to a given member.
    If any members from a given cluster has a distance ε to an outside
    point, that point will be subsequently included to the cluster. This
    process runs continuously until all points are evaluated. Scientific 
    reference for this algorithm is found at:

        Ester, Martin; Kriegel, Hans-Peter; Sander, Jörg; Xu, Xiaowei (1996). 
        Simoudis, Evangelos; Han, Jiawei; Fayyad, Usama M., eds. 
        "A density-based algorithm for discovering clusters in large 
        spatial databases with noise". Proceedings of the Second 
        International Conference on Knowledge Discovery and Data 
        Mining (KDD-96). AAAI Press. pp. 226–231
        Available at: http://www.lsi.upc.edu/~bejar/amlt/material_art/\
                        DM%20clustring%20DBSCAN%20kdd-96.pdf
    
    This reference is also useful for understanding how the algorithm
    works: 
    
        * https://www.naftaliharris.com/blog/visualizing-dbscan-clustering/
    
    Parameters
    ----------
    point_collection : str, default 'sites'
        Table to use take point collection from. This parameter
        may accept dataframes with geographic in the future.

    location_identifier : str, default 'id'
        Location identifier from the point table to use. This
        identifier must be unique to each location.
    
    geometry_identifier : str, default 'geom_point'
        Geometry column to use in computations.

    method : str, default 'kmeans'
        Method to use in clustring. Please seek each method's
        reference for algorithmic information on how they 
        work. Current implementations are:

            * `kmeans`:  Uses a K-means algorithm to select clusters.
                            This method requires the parameter 
                            `number_of_clusters`.

            * `dbscan`:  Uses the DBSCAN algorithm to select clusters.
                            This method requires the parameters
                            `distance_tolerance` and `density_tolerance`.
                            
            * `area`:    Clusters points that are wihin a certain area.
                            Similar to DBSCAN, but without a density value,
                            that is clusters can be of any size. This
                            method requires the parameter `distance_tolerance`.
    
    distance_tolerance : float or int, default 1
        Radius area in km. Area is approximated using a WGS84 degree
        to meter conversion of (distance_tolerance * 1000) / 111195.
        This can include a maximum error of ~0.1%.
    
    density_tolerance : int, default 5
        Minimum number of members that a cluster must have in order
        to exist. If members of a possible cluster do not meet
        this criteria, they will not be assigned a cluster.
        See the `return_no_cluster` parameter.

    number_of_clusters : int, default 5
        Number of clusters to create with the K-means algorithm.
    
    date : str, default None
        If the `point_collection` is either 'sites' or 'cells'
        use this parameter to determing which version of those
        infrastructure elements to use. If the default None is used
        the current date will be used.

    aggregate : bool, default False
        If used, the a dataframe will be returned with the a 
        convex hull geometry per cluster id alongside its 
        centroid. This can be used in conjuction with the
        LocationArea() to create new area representations.

    return_no_cluster : bool, default True
        If used results will include members that have not
        been assigned to a cluster. If this parameter
        is used in conjunction with the `aggregate` parameter,
        elements with no cluster will be ignored. We do not
        recommend using this in conjunction with that
        parameter.
    
    Notes
    --------------
    The DBSCAN implementation method code has originally been sourced from
    Dan Baston's website (implementer of the method in PostGIS) -- 
    the K-mean implementation is a derivation of the DBSCAN implementation:

        * http://www.danbaston.com/posts/2016/06/02/\
            dbscan-clustering-in-postgis.html

    The Area method code has originally been drawn from the 
    GISStackExchange page:

        * https://gis.stackexchange.com/questions/\
            11567/spatial-clustering-with-postgis

    """

    def __init__(
        self,
        point_collection="sites",
        location_identifier="id",
        geometry_identifier="geom_point",
        method="kmeans",
        distance_tolerance=1,
        density_tolerance=5,
        number_of_clusters=5,
        date=None,
        aggregate=False,
        return_no_cluster=True,
    ):

        self.method = method
        self.date = date
        self.aggregate = aggregate
        self.point_collection = point_collection
        self.location_identifier = location_identifier
        self.geometry_identifier = geometry_identifier
        self.return_no_cluster = return_no_cluster
        self.number_of_clusters = number_of_clusters

        # Rough km to degree (WGS84) conversion.
        self.distance_tolerance = (float(distance_tolerance) * 1000) / 111195
        self.density_tolerance = density_tolerance

        if self.point_collection in ("sites", "cells"):
            self.versioned_infrastructure = VersionedInfrastructure(
                table=self.point_collection, date=self.date
            )

        super().__init__()

    @property
    def column_names(self) -> List[str]:
        if self.aggregate:
            return ["cluster_id", "location_id_members", "geom_cluster", "geom"]
        else:
            return ["location_id", "cluster_id", "geom"]

    def __create_aggregate_cluster_geometries(self, sql_points):
        """
        Protected function that is called when we need to
        create an aggregated geometry for all the points
        that are part of a cluster.

        Returns
        -------
        sql: str
            SQL string that aggregates geometries using a
            convex hull, returning the convex hull geometry.
        """
        sql = """

            SELECT
                A.cluster_id,
                array_agg(A.location_id) as location_id_members,
                ST_ConvexHull(ST_Union(A.geom)::geometry)::geography AS geom_cluster,
                ST_Centroid(
                    ST_ConvexHull(ST_Union(A.geom)::geometry)
                )::geography AS geom
            FROM ({sql_points}) AS A
            GROUP BY A.cluster_id

        """.format(
            sql_points=sql_points
        )

        return sql

    def __filter_no_cluster(self, query):
        """
        Private method for wrapping an SQL string
        into another SQL string that filters NULL
        elements. 

        Parameters
        ----------
        query: str
            SQL query to be wrapped. The query
            must contain the column `cluster_id`.
        
        Returns
        -------
        sql: str
            SQL string with that includes a NOT NULL
            statement for filtering `cluster_id`
            elements that have no cluster assigned.
        """
        sql = """

            SELECT
                *
            FROM ({query}) AS B
            WHERE cluster_id IS NOT NULL

        """.format(
            query=query
        )

        return sql

    def __kmeans(self, points):
        """
        Private method that generates SQL string
        for a call to PostGIS' K-means algorithm.

        Parameters
        ----------
        points : str
            SQL string that returns a point statement.
            This string must contain the column specified
            by the parameter `location_identifier`.

        Returns
        -------
        str
            SQL string the clustering computation of an
            area-based algorithm. This algorithm uses
            PostGIS' `ST_ClusterKMeans()` method for
            identifying clusters.
        """
        sql = """

            SELECT 
                {location_identifier} AS location_id,
                ST_ClusterKMeans(geom, 
                                 {number_of_clusters}) OVER () AS cluster_id,
                geom AS geom
            FROM ({points}) AS A

        """.format(
            location_identifier=self.location_identifier,
            points=points,
            number_of_clusters=self.number_of_clusters,
        )

        return sql

    def __dbscan(self, points):
        """
        Private method that generates SQL string
        for a call to PostGIS' DBSCAN algorithm.

        Parameters
        ----------
        points : str
            SQL string that returns a point statement.
            This string must contain the column specified
            by the parameter `location_identifier`.

        Returns
        -------
        sql: str
            SQL string the clustering computation of an
            area-based algorithm. This algorithm uses
            PostGIS' `ST_ClusterDBSCAN()` method for
            identifying clusters.

        """
        logger.info(
            " Running DBSCAN algorithm with parameters "
            + "distance tolerance ({} WGS 84 degrees) ".format(self.distance_tolerance)
            + "and density tolerance of {}.".format(self.density_tolerance)
        )

        sql = """

            SELECT 
                {location_identifier} AS location_id,
                ST_ClusterDBSCAN(geom, 
                                 eps := {epsilon}, 
                                 minPoints := {min_points}) OVER () AS cluster_id,
                geom AS geom
            FROM ({points}) AS A
        
        """.format(
            location_identifier=self.location_identifier,
            points=points,
            epsilon=self.distance_tolerance,
            min_points=self.density_tolerance,
        )

        return sql

    def __area(self, points):
        """
        Private method for generating the SQL of
        the area algorithm. This algorithm will use
        a maximum distance between points as an
        inclusion criteria for a cluster.

        Parameters
        ----------
        points : str
            SQL string that returns a point statement.
            This string must contain the column specified
            by the parameter `location_identifier`.
        
        Returns
        -------
        str
            SQL string the clustering computation of an
            area-based algorithm. This algorithm uses
            PostGIS' `ST_ClusterWithin()` method for
            identifying clusters.

        """
        sql = """

            SELECT
                B.id AS location_id,
                C.cluster_id,
                B.geom AS geom
            FROM (
                SELECT 
                    row_number() OVER () AS cluster_id,
                    geom_collection AS geom_collection
                    FROM (
                        SELECT 
                            unnest(ST_ClusterWithin(A.geom, {distance})) AS geom_collection
                        FROM ({points_table}) AS A
                    ) AS A
            ) AS C,	
            ({points_table}) AS B
            WHERE ST_Within(B.geom, ST_CollectionExtract(C.geom_collection, 1))

        """.format(
            distance=self.distance_tolerance, points_table=points
        )

        return sql

    def _make_query(self):

        if self.point_collection in ("sites", "cells"):
            points_stable_statement = (
                "(" + self.versioned_infrastructure.get_query() + ")"
            )
        else:
            points_stable_statement = self.point_collection

        sql_points_table = """

            SELECT
                {location_identifier},
                {geometry_identifier} AS geom
            FROM {table} AS A

        """.format(
            location_identifier=self.location_identifier,
            geometry_identifier=self.geometry_identifier,
            table=points_stable_statement,
        )

        if self.method == "kmeans":
            sql_cluster_points = self.__kmeans(sql_points_table)

        elif self.method == "dbscan":
            sql_cluster_points = self.__dbscan(sql_points_table)

        elif self.method == "area":
            sql_cluster_points = self.__area(sql_points_table)

        else:
            raise NotImplemented(
                "Method {} has not been implemented.".format(self.method)
            )

        if self.aggregate:
            sql = self.__create_aggregate_cluster_geometries(sql_cluster_points)
        else:
            sql = sql_cluster_points

        if self.return_no_cluster:
            sql = self.__filter_no_cluster(sql)

        return sql

    def _geo_augmented_query(self):
        location_identifier = "location_id"
        geometry_identifier = "geom"
        if self.aggregate:
            location_identifier = "cluster_id"
            geometry_identifier = "geom_cluster"

        sql = """

            SELECT
                {location_identifier} AS gid,
                cluster_id,
                {geometry_identifier} AS geom
            FROM ({base_query}) AS A

        """.format(
            location_identifier=location_identifier,
            geometry_identifier=geometry_identifier,
            base_query=self.get_query(),
        )

        return sql, ["gid", "cluster_id", "geom"]
