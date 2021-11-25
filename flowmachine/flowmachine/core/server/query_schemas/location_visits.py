from marshmallow import fields
from marshmallow.validate import OneOf

from flowmachine.core.server.query_schemas import BaseExposedQuery
from flowmachine.core.server.query_schemas.base_schema import BaseSchema
from flowmachine.core.server.query_schemas.daily_location import DailyLocationSchema
from flowmachine.features.subscriber.location_visits import LocationVisits


class LocationVisitsExposed(BaseExposedQuery):
    def __init__(self, day_trajectories, spatial_unit):
        self.day_trajectories = day_trajectories
        self.spatial_unit = spatial_unit

    def _flowmachine_query_obj(self):
        return LocationVisits(
            day_trajectories=self.day_trajectories,
        )


class LocationVisitsSchema(BaseSchema):
    query_kind = fields.String(validate=OneOf(["location_visits"]))
    day_trajectories = fields.List(fields.Nested(DailyLocationSchema))
