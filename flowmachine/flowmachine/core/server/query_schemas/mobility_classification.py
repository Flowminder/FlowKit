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
    # query_kind class attribute is required for nesting and serialisation
    query_kind = "mobility_classification"

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
    __model__ = MobilityClassificationExposed

    # query_kind parameter is required here for claims validation
    query_kind = fields.String(validate=OneOf([__model__.query_kind]), required=True)
    locations = fields.List(
        fields.Nested(CoalescedLocationSchema), validate=Length(min=1), required=True
    )
    stay_length_threshold = fields.Integer(required=True)

    @validates("locations")
    def validate_locations(self, locations):
        # Filtering out locations with no aggregation_unit attribute -
        # validation errors for these should be raised when validating the
        # individual location query specs
        if (
            len(
                set(
                    location.aggregation_unit
                    for location in locations
                    if hasattr(location, "aggregation_unit")
                )
            )
            > 1
        ):
            raise ValidationError("All locations must have the same aggregation unit")
