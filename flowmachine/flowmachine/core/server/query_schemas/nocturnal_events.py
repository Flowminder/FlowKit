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
from .field_mixins import (
    StartAndEndField,
    EventTypesField,
    SubscriberSubsetField,
)

__all__ = ["NocturnalEventsSchema", "NocturnalEventsExposed"]


class NocturnalEventsExposed(BaseExposedQueryWithSampling):
    def __init__(
        self,
        *,
        start_date,
        end_date,
        night_start_hour,
        night_end_hour,
        event_types,
        subscriber_subset=None,
        sampling=None
    ):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.start = start_date
        self.stop = end_date
        self.hours = (night_start_hour, night_end_hour)
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
    query_kind = fields.String(validate=OneOf(["nocturnal_events"]))
    night_start_hour = fields.Integer(
        validate=Range(0, 23)
    )  # Tuples aren't supported by apispec https://github.com/marshmallow-code/apispec/issues/399
    night_end_hour = fields.Integer(validate=Range(0, 23))

    __model__ = NocturnalEventsExposed
