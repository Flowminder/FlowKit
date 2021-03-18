# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import fields, post_load
from marshmallow.validate import OneOf

from flowmachine.features import EventCount
from .custom_fields import EventTypes, ISODateTime, Hours
from .subscriber_subset import SubscriberSubset
from .base_query_with_sampling import (
    BaseQueryWithSamplingSchema,
    BaseExposedQueryWithSampling,
)
from .field_mixins import (
    HoursField,
    StartAndEndField,
    EventTypesField,
    SubscriberSubsetField,
)

__all__ = ["EventCountSchema", "EventCountExposed"]


class EventCountExposed(BaseExposedQueryWithSampling):
    def __init__(
        self,
        *,
        start_date,
        end_date,
        direction,
        event_types,
        subscriber_subset=None,
        sampling=None,
        hours=None
    ):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.start = start_date
        self.stop = end_date
        self.direction = direction
        self.event_types = event_types
        self.subscriber_subset = subscriber_subset
        self.sampling = sampling
        self.hours = hours

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
            hours=self.hours,
        )


class EventCountSchema(
    StartAndEndField,
    EventTypesField,
    SubscriberSubsetField,
    HoursField,
    BaseQueryWithSamplingSchema,
):
    query_kind = fields.String(validate=OneOf(["event_count"]))
    direction = fields.String(
        required=False, validate=OneOf(["in", "out", "both"]), default="both"
    )  # TODO: use a globally defined enum for this

    __model__ = EventCountExposed
