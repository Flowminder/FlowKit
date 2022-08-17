# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import fields, validates_schema, ValidationError
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
    # query_kind class attribute is required for nesting and serialisation
    query_kind = "coalesced_location"

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
        self.aggregation_unit = preferred_location.aggregation_unit

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
    Schema that exposes CoalescedLocation with a FilteredReferenceLocation
    query as the fallback location
    """

    __model__ = CoalescedLocationExposed

    # query_kind parameter is required here for claims validation
    query_kind = fields.String(validate=OneOf([__model__.query_kind]), required=True)
    preferred_location = fields.Nested(MajorityLocationSchema, required=True)
    fallback_location = fields.Nested(MajorityLocationSchema, required=True)
    subscriber_location_weights = fields.Nested(LocationVisitsSchema, required=True)
    weight_threshold = fields.Integer(required=True)

    @validates_schema(skip_on_field_errors=True)
    def validate_aggregation_units(self, data, **kwargs):
        """
        Validate that preferred_location, fallback_location and
        subscriber_location_weights all have the same aggregation unit
        """
        errors = {}

        if (
            data["fallback_location"].aggregation_unit
            != data["preferred_location"].aggregation_unit
        ):
            errors["fallback_location"] = [
                "fallback_location must have the same aggregation_unit as preferred_location"
            ]

        if (
            data["subscriber_location_weights"].aggregation_unit
            != data["fallback_location"].aggregation_unit
        ):
            errors["subscriber_location_weights"] = [
                "subscriber_location_weights must have the same aggregation_unit as fallback_location"
            ]

        if errors:
            raise ValidationError(errors)
