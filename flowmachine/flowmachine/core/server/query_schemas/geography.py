# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import fields
from marshmallow.validate import OneOf

from flowmachine.features import Geography
from .base_exposed_query import BaseExposedQuery
from .aggregation_unit import AggregationUnitMixin

__all__ = ["GeographySchema", "GeographyExposed"]

from .base_schema import BaseSchema


class GeographyExposed(BaseExposedQuery):
    def __init__(self, *, aggregation_unit):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.aggregation_unit = aggregation_unit

    @property
    def _flowmachine_query_obj(self):
        """
        Return the underlying flowmachine daily_location object.

        Returns
        -------
        Query
        """
        return Geography(self.aggregation_unit)

    @property
    def geojson_sql(self):
        """
        Return a SQL string for getting the geography as GeoJSON.
        """
        # Explicitly project to WGS84 (SRID=4326) to conform with GeoJSON standard
        sql = self._flowmachine_query_obj.geojson_query(crs=4326)
        return sql


class GeographySchema(AggregationUnitMixin, BaseSchema):
    # query_kind parameter is required here for claims validation
    query_kind = fields.String(validate=OneOf(["geography"]))

    __model__ = GeographyExposed
