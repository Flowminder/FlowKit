# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import fields, validates, ValidationError
from marshmallow.validate import OneOf, Length
from marshmallow_oneofschema import OneOfSchema

from flowmachine.core.server.query_schemas import BaseExposedQuery
from flowmachine.core.server.query_schemas.base_query_with_sampling import (
    BaseQueryWithSamplingSchema,
    BaseExposedQueryWithSampling,
)
from flowmachine.core.server.query_schemas.base_schema import BaseSchema
from flowmachine.core.server.query_schemas.daily_location import DailyLocationSchema
from flowmachine.core.server.query_schemas.modal_location import ModalLocationSchema
from flowmachine.features import DayTrajectories
from flowmachine.features.subscriber.location_visits import LocationVisits


class LocationVisitsExposed(BaseExposedQueryWithSampling):
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


class VisitableLocation(OneOfSchema):
    type_field = "query_kind"
    type_schemas = {
        "daily_location": DailyLocationSchema,
        "modal_location": ModalLocationSchema,
    }


class LocationVisitsSchema(BaseQueryWithSamplingSchema):
    query_kind = fields.String(validate=OneOf(["location_visits"]))
    locations = fields.List(fields.Nested(VisitableLocation), validate=Length(min=1))

    @validates("locations")
    def validate_locations(self, locations):
        if len(set(location.aggregation_unit for location in locations)) > 1:
            raise ValidationError("All locations must have the same aggregation unit")

    __model__ = LocationVisitsExposed
