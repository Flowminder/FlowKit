# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Mixin providing utility methods for geographic type queries.



"""
import rapidjson as json


from flowmachine.utils import proj4string

import structlog

logger = structlog.get_logger("flowmachine.debug", submodule=__name__)


try:
    import geopandas
except ImportError:
    logger.debug("GeoPandas not found. `to_geopandas` unavailable.")

from ...core.query import Query


class GeoDataMixin:
    """
    Mixin providing utility methods specific to geographic
    type queries.
    """

    def _get_location_join(self):
        """
        Utility method which searches the query tree for location
        information.
        
        Returns
        -------
        JoinToLocation
            The first JoinToLocation object encountered in the query tree.

        """
        open = set(self.dependencies)
        closed = set()
        while True:
            try:
                qur = open.pop()
                closed.add(qur)

                #
                #  This will check if the passed query
                #  is an instance of the JoinToLocation class.
                #  We don't check for the instance directly
                #  because of import issues. This isn't an
                #  ideal solution.
                #
                #  - Luis Capelo, June 22, 2017
                #
                if "JoinToLocation" in str(getattr(qur, "__class__", lambda: None)):
                    return qur

                open.update(qur.dependencies - closed)

            except KeyError:
                logger.warning("No JoinToLocation object found.")
                break

    def _geo_augmented_query(self):
        """
        Creates a version of this query augmented with a geom column,
        and a gid column, performing any aggregation necessary to do
        this sensibly.
        
        Returns
        ------
        str
            A version of this query with geom and gid columns
        list
            The columns this query contains
        """
        loc_join = self._get_location_join()
        level = loc_join.level
        if level == "lat-lon":
            # Need to recreate a point
            joined_query = """

                SELECT 
                    row_number() over() AS gid,
                    *, 
                    ST_SetSRID(ST_Point(lon, lat), 4326) AS geom
                FROM ({}) AS L

            """.format(
                self.get_query()
            )
        elif level == "versioned-site":
            joined_query = """

                SELECT 
                    row_number() OVER () AS gid, 
                    geom_point AS geom, 
                    U.*
                FROM ({qur}) AS U
                LEFT JOIN infrastructure.sites AS S
                    ON U.site_id = S.id AND
                       U.version = S.version

            """.format(
                qur=self.get_query()
            )
        elif level == "versioned-cell":
            joined_query = """

                SELECT 
                    row_number() OVER () AS gid, 
                    geom_point AS geom, 
                    U.*
                FROM ({qur}) AS U
                LEFT JOIN infrastructure.cells AS S
                    ON U.location_id = S.id AND
                       U.version = S.version

            """.format(
                qur=self.get_query()
            )
        else:
            mapping = loc_join.right_query.mapping
            col_name = mapping.column_name[0]
            geom_col = mapping.geom_col
            poly_query = mapping.polygon_table

            #
            #  Same as comment above on JoinToLocations.
            #  This also needs to deal with grids. This
            #  solution isn't good.
            #
            if isinstance(poly_query, Query):
                sql_polygon_query = poly_query.get_query()

            else:
                sql_polygon_query = "SELECT * FROM {}".format(poly_query)

            joined_query = """

                SELECT 
                    row_number() OVER () as gid, 
                    {geom_col} as geom, 
                    U.*
                FROM ({qur}) AS U
                LEFT JOIN ({poly_query}) AS G
                    ON U.{l_col_name} = G.{r_col_name}

            """.format(
                qur=self.get_query(),
                poly_query=sql_polygon_query,
                geom_col=geom_col,
                l_col_name=self.column_names[0],
                r_col_name=col_name,
            )
        cols = list(set(self.column_names + ["gid", "geom"]))
        return joined_query, cols

    def geojson_query(self, crs=None):
        """
        Create a query which will transform each row into a geojson
        feature.
        
        Parameters
        ----------
        crs : int or str
            Optionally give an integer srid, or valid proj4 string to transform output to 
        
        Returns
        -------
        str
            A json aggregation query string
        """
        crs_trans = "geom"
        if crs:
            crs_trans = "ST_Transform(geom::geometry, {0!r})".format(crs)
        joined_query, cols = self._geo_augmented_query()
        properties = [f"'{col}', {col}" for col in cols if col not in ("geom", "gid")]
        properties.append(
            f"'centroid', ST_AsGeoJSON(ST_Centroid({crs_trans}::geometry))::json"
        )

        json_query = f"""
                SELECT
                    'Feature' AS type,
                    gid AS id,
                    ST_AsGeoJSON({crs_trans})::json AS geometry,
                    json_build_object({", ".join(properties)}) AS properties
                FROM (SELECT * FROM ({joined_query}) AS J) AS row
        """

        return json_query

    def to_geojson_file(self, filename, crs=None):
        """
        Export this query to a GeoJson FeatureCollection file.
        
        Parameters
        ----------
        filename : str
            File to save resulting geojson as.
        crs : int or str
            Optionally give an integer srid, or valid proj4 string to transform output to
        """
        with open(filename, "w") as fout:
            json.dump(self.to_geojson(crs=crs), fout)

    def to_geojson_string(self, crs=None):
        """
        Parameters
        ----------
        crs : int or str
            Optionally give an integer srid, or valid proj4 string to transform output to
        
        Returns
        -------
        str
            A string containing the this query as a GeoJson FeatureCollection.
        """
        return json.dumps(self.to_geojson(crs=crs))

    def _get_geojson(self, proj4):
        """
        Helper function that actually retrieves geojson from the
        database, combines into a geojson featurecollection, and
        sets a proj4 string on it.


        Parameters
        ----------
        proj4 : str
            Valid proj4 string to project to.

        Returns
        -------
        dict

        """
        features = [
            {"type": x[0], "id": x[1], "geometry": x[2], "properties": x[3]}
            for x in self.connection.fetch(self.geojson_query(crs=proj4))
        ]
        js = {
            "properties": {"crs": proj4},
            "type": "FeatureCollection",
            "features": features,
        }
        return js

    def to_geojson(self, crs=None):
        """
        Parameters
        ----------
        crs : int or str
            Optionally give an integer srid, or valid proj4 string to transform output to
        
        Returns
        -------
        dict
            This query as a GeoJson FeatureCollection in dict form.
        """
        proj4_string = proj4string(self.connection, crs)
        try:
            js = self._geojson.get(proj4_string, self._get_geojson(proj4_string))
        except AttributeError:
            self._geojson = {}
            js = self._get_geojson(proj4_string)
        if self._cache:
            self._geojson[proj4_string] = js
        return js.copy()

    def __getstate__(self):
        """
        Removes properties which should not be pickled, or hashed. Override
        this method in your subclass if you need to add more.

        Overridden to remove geojson storage.

        Returns
        -------
        dict
            A picklable and hash-safe copy of this objects internal dict.
        """
        state = super().__getstate__()
        try:
            del state["_geojson"]
        except:
            pass  # Not a problem if it doesn't exist
        return state

    def turn_off_caching(self):
        """
        Turn off caching. Overridden to also remove cached geojson.
        """
        self._geojson = {}
        super().turn_off_caching()

    def to_geopandas(self, crs=None):
        """
        Parameters
        ----------
        crs : int or str
            Optionally give an integer srid, or valid proj4 string to transform output to

        Returns
        -------
        GeoDataFrame
            This query as a GeoPandas GeoDataFrame.
        """

        js = self.to_geojson(crs=crs)
        gdf = geopandas.GeoDataFrame.from_features(js["features"])
        gdf.crs = js["properties"]["crs"]

        return gdf
