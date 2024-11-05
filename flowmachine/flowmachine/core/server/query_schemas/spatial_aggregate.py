# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import fields
from marshmallow.validate import OneOf

from flowmachine.features.location.redacted_spatial_aggregate import (
    RedactedSpatialAggregate,
)
from flowmachine.features.location.spatial_aggregate import SpatialAggregate
from .aggregation_unit import AggregationUnitKind
from .base_exposed_query import BaseExposedQuery
from .base_schema import BaseSchema

__all__ = [
    "SpatialAggregateSchema",
    "SpatialAggregateExposed",
]

from .reference_location import ReferenceLocationSchema


class SpatialAggregateExposed(BaseExposedQuery):
    # query_kind class attribute is required for nesting and serialisation
    query_kind = "spatial_aggregate"

    def __init__(self, *, locations):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.locations = locations

    @property
    def aggregation_unit(self):
        return self.locations.aggregation_unit

    @property
    def _flowmachine_query_obj(self):
        """
        Return the underlying flowmachine object.

        Returns
        -------
        Query
        """
        locations = self.locations._flowmachine_query_obj
        return RedactedSpatialAggregate(
            spatial_aggregate=SpatialAggregate(locations=locations)
        )


class SpatialAggregateSchema(BaseSchema):
    __model__ = SpatialAggregateExposed

    # query_kind parameter is required here for claims validation
    query_kind = fields.String(validate=OneOf([__model__.query_kind]), required=True)
    aggregation_unit = AggregationUnitKind(dump_only=True)
    locations = fields.Nested(ReferenceLocationSchema, required=True)
