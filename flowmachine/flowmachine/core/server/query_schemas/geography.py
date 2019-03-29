# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import Schema, post_load

from flowmachine.core.geotable import GeoTable
from .base_exposed_query import BaseExposedQuery
from .custom_fields import AggregationUnit

__all__ = ["GeographySchema", "GeographyExposed"]


class GeographySchema(Schema):
    aggregation_unit = AggregationUnit()

    @post_load
    def make_query_object(self, params):
        return GeographyExposed(**params)


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
        return GeoTable(
            name=self.aggregation_unit,
            schema="geography",
            columns=[
                f"{self.aggregation_unit}name",
                f"{self.aggregation_unit}pcod",
                "geom",
            ],
        )
