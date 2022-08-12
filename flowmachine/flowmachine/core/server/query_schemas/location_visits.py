# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import fields, validates, ValidationError
from marshmallow.validate import OneOf, Length

from flowmachine.core.server.query_schemas.base_query_with_sampling import (
    BaseQueryWithSamplingSchema,
    BaseExposedQueryWithSampling,
)
from flowmachine.core.server.query_schemas.daily_location import DailyLocationSchema
from flowmachine.core.server.query_schemas.modal_location import ModalLocationSchema
from flowmachine.features import DayTrajectories
from flowmachine.features.subscriber.location_visits import LocationVisits
from .one_of_query import OneOfQuerySchema


class LocationVisitsExposed(BaseExposedQueryWithSampling):
    # query_kind class attribute is required for nesting and serialisation
    query_kind = "location_visits"

    def __init__(self, locations, *, sampling=None):
        self.locations = locations
        self.sampling = sampling
        self.aggregation_unit = locations[0].aggregation_unit

    @property
    def _unsampled_query_obj(self):
        return LocationVisits(
            day_trajectories=DayTrajectories(
                *[exp_query._flowmachine_query_obj for exp_query in self.locations]
            ),
        )


class VisitableLocation(OneOfQuerySchema):
    query_schemas = (DailyLocationSchema, ModalLocationSchema)


class LocationVisitsSchema(BaseQueryWithSamplingSchema):
    __model__ = LocationVisitsExposed

    # query_kind parameter is required here for claims validation
    query_kind = fields.String(validate=OneOf([__model__.query_kind]), required=True)
    locations = fields.List(
        fields.Nested(VisitableLocation), validate=Length(min=1), required=True
    )

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
