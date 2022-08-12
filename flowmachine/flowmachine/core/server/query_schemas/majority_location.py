# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
from marshmallow import fields
from marshmallow.validate import OneOf

from flowmachine.core.server.query_schemas.base_query_with_sampling import (
    BaseQueryWithSamplingSchema,
    BaseExposedQueryWithSampling,
)
from flowmachine.core.server.query_schemas.location_visits import LocationVisitsSchema
from flowmachine.features.subscriber.majority_location import MajorityLocation

from .one_of_query import OneOfQuerySchema


class MajorityLocationExposed(BaseExposedQueryWithSampling):
    # query_kind class attribute is required for nesting and serialisation
    query_kind = "majority_location"

    def __init__(
        self, *, subscriber_location_weights, include_unlocatable, sampling=None
    ):
        self.subscriber_location_weights = subscriber_location_weights
        self.include_unlocatable = include_unlocatable
        self.sampling = sampling
        self.aggregation_unit = subscriber_location_weights.aggregation_unit

    @property
    def _unsampled_query_obj(self):
        return MajorityLocation(
            subscriber_location_weights=self.subscriber_location_weights._flowmachine_query_obj,
            weight_column="value",
            include_unlocatable=self.include_unlocatable,
        )


class WeightedLocationQueries(OneOfQuerySchema):
    query_schemas = (LocationVisitsSchema,)


class MajorityLocationSchema(BaseQueryWithSamplingSchema):
    __model__ = MajorityLocationExposed

    # query_kind parameter is required here for claims validation
    query_kind = fields.String(validate=OneOf([__model__.query_kind]), required=True)
    subscriber_location_weights = fields.Nested(WeightedLocationQueries, required=True)
    include_unlocatable = fields.Boolean(missing=False)
