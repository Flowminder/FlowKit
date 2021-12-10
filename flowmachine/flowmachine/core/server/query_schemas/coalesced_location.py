# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import fields
from marshmallow.validate import OneOf

from flowmachine.core.server.query_schemas.base_query_with_sampling import (
    BaseExposedQueryWithSampling,
    BaseQueryWithSamplingSchema,
)
from flowmachine.core.server.query_schemas.location_visits import LocationVisitsSchema
from flowmachine.core.server.query_schemas.majority_location import (
    MajorityLocationSchema,
)
from flowmachine.features.subscriber.coalesced_location import CoalescedLocation
from flowmachine.features.subscriber.filtered_reference_location import (
    FilteredReferenceLocation,
)


class CoalescedLocationExposed(BaseExposedQueryWithSampling):
    def __init__(
        self,
        preferred_location,
        fallback_location,
        subscriber_location_weights,
        weight_threshold: int,
        sampling=None,
    ):
        self.preferred_location = preferred_location
        self.fallback_location = fallback_location
        self.subscriber_location_weights = subscriber_location_weights
        self.weight_threshold = weight_threshold
        self.sampling = sampling

    @property
    def _unsampled_query_obj(self):
        return CoalescedLocation(
            preferred_locations=self.preferred_location._flowmachine_query_obj,
            fallback_locations=FilteredReferenceLocation(
                reference_locations_query=self.fallback_location._flowmachine_query_obj,
                filter_query=self.subscriber_location_weights._flowmachine_query_obj,
                lower_bound=self.weight_threshold,
            ),
        )


class CoalescedLocationSchema(BaseQueryWithSamplingSchema):
    """
    Schema that exposes CoalescedLocation with a FilteredReferenceLocation query as the fallback location
    """

    query_kind = fields.String(validate=OneOf(["coalesced_location"]))
    preferred_location = fields.Nested(MajorityLocationSchema, required=True)
    fallback_location = fields.Nested(MajorityLocationSchema, required=True)
    subscriber_location_weights = fields.Nested(LocationVisitsSchema, required=True)
    weight_threshold = fields.Integer(required=True)

    __model__ = CoalescedLocationExposed
