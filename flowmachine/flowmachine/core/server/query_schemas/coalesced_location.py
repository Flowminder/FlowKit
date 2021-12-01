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
            preferred_locations=self.preferred_location._unsampled_query_obj,
            fallback_locations=FilteredReferenceLocation(
                reference_locations_query=self.fallback_location._unsampled_query_obj,
                filter_query=self.subscriber_location_weights._unsampled_query_obj,
                lower_bound=self.weight_threshold,
            ),
        )


# Wanted this to be
class SubscriberLocationMappingQueries(OneOfSchema):
    """
    A set of queries that return a mapping between unique subscribers and locations
    """

    type_field = "query_kind"
    type_schemas = {
        "daily_location": DailyLocationSchema,
        "modal_location": ModalLocationSchema,
        "most_frequent_location": MostFrequentLocationSchema,
        "visited_most_days": VisitedMostDaysSchema,
        "majority_location": MajorityLocationSchema,
    }


class SubscriberWeightMappingQueries(OneOfSchema):
    """
    Set of queries that map a subscriber to a set of weights
    """

    type_field = "query_kind"
    type_schemas = {
        "location_visits": LocationVisitsSchema,
        # These don't seem to be exposed yet
        # "call_days": CallDaysSchema
        # "per_location_subscriber_call_duration" :PerLocationSubscriberCallDurationSchema
    }


class CoalescedLocationSchema(BaseQueryWithSamplingSchema):
    """
    Schema that exposes CoalescedLocation
    """

    query_kind = fields.String(validate=OneOf(["coalesced_location"]))
    preferred_location = fields.Nested(SubscriberLocationMappingQueries)
    fallback_location = fields.Nested(SubscriberLocationMappingQueries)
    subscriber_location_weights = fields.Nested(SubscriberWeightMappingQueries)
    weight_threshold = fields.Integer()

    __model__ = CoalescedLocationExposed
