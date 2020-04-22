# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import fields, post_load
from marshmallow.validate import OneOf

from flowmachine.features import EventCount
from .custom_fields import EventTypes, SubscriberSubset, ISODateTime
from .base_query_with_sampling import (
    BaseQueryWithSamplingSchema,
    BaseExposedQueryWithSampling,
)

__all__ = ["EventCountSchema", "EventCountExposed"]


class EventCountExposed(BaseExposedQueryWithSampling):
    def __init__(
        self,
        *,
        start,
        stop,
        direction,
        event_types,
        subscriber_subset=None,
        sampling=None
    ):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.start = start
        self.stop = stop
        self.direction = direction
        self.event_types = event_types
        self.subscriber_subset = subscriber_subset
        self.sampling = sampling

    @property
    def _unsampled_query_obj(self):
        """
        Return the underlying flowmachine event_count object.

        Returns
        -------
        Query
        """
        return EventCount(
            start=self.start,
            stop=self.stop,
            direction=self.direction,
            tables=self.event_types,
            subscriber_subset=self.subscriber_subset,
        )


class EventCountSchema(BaseQueryWithSamplingSchema):
    query_kind = fields.String(validate=OneOf(["event_count"]))
    start = ISODateTime(required=True)
    stop = ISODateTime(required=True)
    direction = fields.String(
        required=False, validate=OneOf(["in", "out", "both"]), default="both"
    )  # TODO: use a globally defined enum for this
    event_types = EventTypes()
    subscriber_subset = SubscriberSubset()

    __model__ = EventCountExposed
