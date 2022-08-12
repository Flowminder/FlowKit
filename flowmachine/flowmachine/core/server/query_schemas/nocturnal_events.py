# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import fields
from marshmallow.validate import OneOf, Range

from flowmachine.features import NocturnalEvents

from .base_query_with_sampling import (
    BaseQueryWithSamplingSchema,
    BaseExposedQueryWithSampling,
)
from .custom_fields import Hours
from .field_mixins import (
    StartAndEndField,
    EventTypesField,
    SubscriberSubsetField,
)

__all__ = ["NocturnalEventsSchema", "NocturnalEventsExposed"]


class NocturnalEventsExposed(BaseExposedQueryWithSampling):
    # query_kind class attribute is required for nesting and serialisation
    query_kind = "nocturnal_events"

    def __init__(
        self,
        *,
        start_date,
        end_date,
        night_hours,
        event_types,
        subscriber_subset=None,
        sampling=None
    ):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.start = start_date
        self.stop = end_date
        self.hours = night_hours
        self.event_types = event_types
        self.subscriber_subset = subscriber_subset
        self.sampling = sampling

    @property
    def _unsampled_query_obj(self):
        """
        Return the underlying flowmachine nocturnal_events object.

        Returns
        -------
        Query
        """
        return NocturnalEvents(
            start=self.start,
            stop=self.stop,
            hours=self.hours,
            tables=self.event_types,
            subscriber_subset=self.subscriber_subset,
        )


class NocturnalEventsSchema(
    StartAndEndField,
    EventTypesField,
    SubscriberSubsetField,
    BaseQueryWithSamplingSchema,
):
    __model__ = NocturnalEventsExposed

    # query_kind parameter is required here for claims validation
    query_kind = fields.String(validate=OneOf([__model__.query_kind]), required=True)
    night_hours = fields.Nested(Hours, required=True)
