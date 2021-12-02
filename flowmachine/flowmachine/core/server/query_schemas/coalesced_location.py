from marshmallow import fields, Schema
from marshmallow.validate import OneOf
from marshmallow_oneofschema import OneOfSchema

from flowmachine.core.server.query_schemas.base_query_with_sampling import (
    BaseExposedQueryWithSampling,
    BaseQueryWithSamplingSchema,
)
from flowmachine.core.server.query_schemas.daily_location import DailyLocationSchema
from flowmachine.core.server.query_schemas.location_visits import LocationVisitsSchema
from flowmachine.core.server.query_schemas.majority_location import (
    MajorityLocationSchema,
)
from flowmachine.core.server.query_schemas.modal_location import ModalLocationSchema
from flowmachine.core.server.query_schemas.most_frequent_location import (
    MostFrequentLocationSchema,
)

from flowmachine.core.server.query_schemas.visited_most_days import (
    VisitedMostDaysSchema,
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
    Schema that exposes CoalescedLocation with a specific fallback
    """

    query_kind = fields.String(validate=OneOf(["coalesced_location"]))
    preferred_location = fields.Nested(MajorityLocationSchema)
    fallback_location = fields.Nested(MajorityLocationSchema)
    subscriber_location_weights = fields.Nested(LocationVisitsSchema)
    weight_threshold = fields.Integer()

    __model__ = CoalescedLocationExposed
