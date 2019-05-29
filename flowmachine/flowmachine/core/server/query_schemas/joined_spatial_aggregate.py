# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import Schema, fields, post_load
from marshmallow.validate import OneOf
from marshmallow_oneofschema import OneOfSchema

from flowmachine.core.server.query_schemas.radius_of_gyration import (
    RadiusOfGyrationSchema,
)
from flowmachine.core.server.query_schemas.spatial_aggregate import (
    InputToSpatialAggregate,
)
from flowmachine.features.utilities.spatial_aggregates import JoinedSpatialAggregate
from .base_exposed_query import BaseExposedQuery


__all__ = ["JoinedSpatialAggregateSchema", "JoinedSpatialAggregateExposed"]


class JoinableMetrics(OneOfSchema):
    type_field = "query_kind"
    type_schemas = {"radius_of_gyration": RadiusOfGyrationSchema}


class JoinedSpatialAggregateSchema(Schema):
    query_kind = fields.String(validate=OneOf(["joined_spatial_aggregate"]))
    locations = fields.Nested(InputToSpatialAggregate, required=True)
    metric = fields.Nested(JoinableMetrics, required=True)
    method = fields.String(
        default="mean", validate=OneOf(JoinedSpatialAggregate.allowed_methods)
    )

    @post_load
    def make_query_object(self, params):
        return JoinedSpatialAggregateExposed(**params)


class JoinedSpatialAggregateExposed(BaseExposedQuery):
    def __init__(self, *, locations, metric, method):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.locations = locations
        self.metric = metric
        self.method = method

    @property
    def _flowmachine_query_obj(self):
        """
        Return the underlying flowmachine object.

        Returns
        -------
        Query
        """
        locations = self.locations._flowmachine_query_obj
        metric = self.metric._flowmachine_query_obj
        return JoinedSpatialAggregate(
            locations=locations, metric=metric, method=self.method
        )
