from marshmallow import fields, validates, ValidationError
from marshmallow.validate import OneOf
from marshmallow_oneofschema import OneOfSchema

from flowmachine.core.server.query_schemas import BaseExposedQuery
from flowmachine.core.server.query_schemas.base_schema import BaseSchema
from flowmachine.core.server.query_schemas.daily_location import DailyLocationSchema
from flowmachine.core.server.query_schemas.modal_location import ModalLocationSchema
from flowmachine.features import DayTrajectories
from flowmachine.features.subscriber.location_visits import LocationVisits


class LocationVisitsExposed(BaseExposedQuery):
    def __init__(self, day_trajectories):
        self.day_trajectories = day_trajectories

    def _flowmachine_query_obj(self):
        return LocationVisits(
            day_trajectories=DayTrajectories(
                *[
                    exp_query._flowmachine_query_obj
                    for exp_query in self.day_trajectories
                ]
            ),
        )


class VisitableLocation(OneOfSchema):
    type_field = "query_kind"
    type_schemas = {
        "daily_location": DailyLocationSchema,
        "modal_location": ModalLocationSchema,
    }


class LocationVisitsSchema(BaseSchema):
    query_kind = fields.String(validate=OneOf(["location_visits"]))
    day_trajectories = fields.List(fields.Nested(VisitableLocation))
    __model__ = LocationVisitsExposed
