# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import Schema, fields, post_load
from marshmallow.validate import OneOf, Length
from marshmallow_oneofschema import OneOfSchema

from flowmachine.features import Displacement
from .base_exposed_query import BaseExposedQuery
from .custom_fields import SubscriberSubset, Statistic
from .daily_location import DailyLocationSchema, DailyLocationExposed


__all__ = ["DisplacementSchema", "DisplacementExposed"]


class InputToDisplacementSchema(OneOfSchema):
    type_field = "query_kind"
    type_schemas = {"daily_location": DailyLocationSchema}


class DisplacementSchema(Schema):
    query_kind = fields.String(validate=OneOf(["displacement"]))
    start = fields.Date(required=True)
    stop = fields.Date(required=True)
    value = Statistic()
    reference_location = fields.Nested(InputToDisplacementSchema, many=False)
    subscriber_subset = SubscriberSubset()

    @post_load
    def make_query_object(self, params, **kwargs):
        return DisplacementExposed(**params)


class DisplacementExposed(BaseExposedQuery):
    def __init__(
        self, *, start, stop, value, reference_location, subscriber_subset=None
    ):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.start = start
        self.stop = stop
        self.value = value
        self.reference_location = reference_location
        self.subscriber_subset = subscriber_subset

    @property
    def _flowmachine_query_obj(self):
        """
        Return the underlying flowmachine displacement object.

        Returns
        -------
        Query
        """
        return Displacement(
            start=self.start,
            stop=self.stop,
            value=self.value,
            reference_location=self.reference_location,
            subscriber_subset=self.subscriber_subset,
        )
