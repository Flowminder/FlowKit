from marshmallow import fields, Schema
from marshmallow.validate import OneOf
from marshmallow_oneofschema import OneOfSchema

from flowmachine.core.server.query_schemas.base_query_with_sampling import (
    BaseExposedQueryWithSampling,
    BaseQueryWithSamplingSchema,
)
from flowmachine.core.server.query_schemas.location_visits import LocationVisitsSchema
from flowmachine.core.server.query_schemas.majority_location import (
    MajorityLocationSchema,
)
from flowmachine.core.server.query_schemas.modal_location import ModalLocationSchema
from flowmachine.features.subscriber.coalesced_location import CoalescedLocation
from flowmachine.features.subscriber.filtered_reference_location import (
    FilteredReferenceLocation,
)
from flowmachine.features.subscriber.majority_location import MajorityLocation


class CoalescedLocationExposed(BaseExposedQueryWithSampling):
    def __init__(
        self,
        preferred_location: MajorityLocation,
        fallback_location: MajorityLocation,
        subscriber_location_weights,
        weight_threshold: int,
    ):
        self.preferred_location = preferred_location
        self.fallback_location = fallback_location
        self.subscriber_location_weights = subscriber_location_weights
        self.weight_threshold = weight_threshold

    def _unsampled_query_obj(self):
        return CoalescedLocation(
            preferred_locations=self.preferred_location,
            fallback_locations=FilteredReferenceLocation(
                reference_locations_query=self.fallback_location,
                filter_query=self.subscriber_location_weights,
                lower_bound=self.weight_threshold,
            ),
        )


# List of queries that map a set of unique subscribers to a set of locations
class SubscriberLocationMappingQueries(OneOfSchema):
    """
    Set of queries that map a set of unique subscribers to a set of locations
    """

    type_field = "query_kind"
    type_schemas = {
        "majority_location": MajorityLocationSchema,
        "modal_location": ModalLocationSchema
        # More here?
    }


class SubscriberWeightMappingQueries(OneOfSchema):
    """
    Set of queries that map a subscriber to a set of weights
    """

    type_field = "query_kind"
    type_schemas = {
        "location_visits": LocationVisitsSchema,
        # More here, too?
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
