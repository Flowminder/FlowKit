# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import fields, post_load
from marshmallow.validate import OneOf, Length
from marshmallow_oneofschema import OneOfSchema

from .custom_fields import SubscriberSubset
from .aggregation_unit import AggregationUnit
from .daily_location import DailyLocationSchema, DailyLocationExposed
from .base_query_with_sampling import (
    BaseQueryWithSamplingSchema,
    BaseExposedQueryWithSampling,
)


class InputToModalLocationSchema(OneOfSchema):
    type_field = "query_kind"
    type_schemas = {"daily_location": DailyLocationSchema}


class ModalLocationSchema(BaseQueryWithSamplingSchema):
    # query_kind parameter is required here for claims validation
    query_kind = fields.String(validate=OneOf(["modal_location"]))
    locations = fields.Nested(
        InputToModalLocationSchema, many=True, validate=Length(min=1)
    )
    aggregation_unit = AggregationUnit(required=True)
    subscriber_subset = SubscriberSubset(required=False)

    @post_load
    def make_query_object(self, data, **kwargs):
        return ModalLocationExposed(**data)


class ModalLocationExposed(BaseExposedQueryWithSampling):
    def __init__(
        self, locations, *, aggregation_unit, subscriber_subset=None, sampling=None
    ):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.locations = locations
        self.aggregation_unit = aggregation_unit
        self.subscriber_subset = subscriber_subset
        self.sampling = sampling

    @property
    def _unsampled_query_obj(self):
        """
        Return the underlying flowmachine ModalLocation object.

        Returns
        -------
        ModalLocation
        """
        from flowmachine.features import ModalLocation

        locations = [loc._flowmachine_query_obj for loc in self.locations]
        return ModalLocation(*locations)
