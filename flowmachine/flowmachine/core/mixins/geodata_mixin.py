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

from ..context import get_db

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
        join_columns_string = ",".join(self.spatial_unit.location_id_columns)

        self.spatial_unit.verify_criterion("has_geography")

        sql = f"""
        SELECT
            row_number() over() AS gid,
            *
        FROM ({self.get_query()}) AS Q
        LEFT JOIN ({self.spatial_unit.get_geom_query()}) AS G
        USING ({join_columns_string})
        """
        cols = list(set(self.column_names + ["gid", "geom"]))
        return sql, cols

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
            for x in get_db().fetch(self.geojson_query(crs=proj4))
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
        proj4_string = proj4string(get_db(), crs)
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
