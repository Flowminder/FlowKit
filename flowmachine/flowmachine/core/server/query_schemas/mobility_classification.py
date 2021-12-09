# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import fields, validates, ValidationError
from marshmallow.validate import OneOf, Length

from flowmachine.core.server.query_schemas.base_query_with_sampling import (
    BaseQueryWithSamplingSchema,
    BaseExposedQueryWithSampling,
)
from flowmachine.core.server.query_schemas.coalesced_location import (
    CoalescedLocationSchema,
)
from flowmachine.features.subscriber.mobility_classification import (
    MobilityClassification,
)


class MobilityClassificationExposed(BaseExposedQueryWithSampling):
    def __init__(self, *, locations, stay_length_threshold, sampling=None):
        self.locations = locations
        self.stay_length_threshold = stay_length_threshold
        self.sampling = sampling

    @property
    def _unsampled_query_obj(self):
        return MobilityClassification(
            locations=[loc._flowmachine_query_obj for loc in self.locations],
            stay_length_threshold=self.stay_length_threshold,
        )


class MobilityClassificationSchema(BaseQueryWithSamplingSchema):
    query_kind = fields.String(validate=OneOf(["mobility_classification"]))
    locations = fields.List(
        fields.Nested(CoalescedLocationSchema), validate=Length(min=1)
    )
    stay_length_threshold = fields.Integer(required=True)

    @validates("locations")
    def validate_locations(self, locations):
        if len(set(location.aggregation_unit for location in locations)) > 1:
            raise ValidationError("All locations must have the same aggregation unit")

    __model__ = MobilityClassificationExposed
