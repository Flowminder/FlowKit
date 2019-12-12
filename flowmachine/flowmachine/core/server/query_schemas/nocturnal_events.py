# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import fields, post_load
from marshmallow.validate import OneOf, Length, Range

from flowmachine.features import NocturnalEvents
from .custom_fields import SubscriberSubset
from .base_query_with_sampling import (
    BaseQueryWithSamplingSchema,
    BaseExposedQueryWithSampling,
)

__all__ = ["NocturnalEventsSchema", "NocturnalEventsExposed"]


class NocturnalEventsSchema(BaseQueryWithSamplingSchema):
    query_kind = fields.String(validate=OneOf(["nocturnal_events"]))
    start = fields.Date(required=True)
    stop = fields.Date(required=True)
    night_start_hour = fields.Integer(
        validate=Range(0, 23)
    )  # Tuples aren't supported by apispec https://github.com/marshmallow-code/apispec/issues/399
    night_end_hour = fields.Integer(validate=Range(0, 23))
    subscriber_subset = SubscriberSubset()

    @post_load
    def make_query_object(self, params, **kwargs):
        return NocturnalEventsExposed(**params)


class NocturnalEventsExposed(BaseExposedQueryWithSampling):
    def __init__(
        self,
        *,
        start,
        stop,
        night_start_hour,
        night_end_hour,
        subscriber_subset=None,
        sampling=None
    ):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.start = start
        self.stop = stop
        self.hours = (night_start_hour, night_end_hour)
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
            subscriber_subset=self.subscriber_subset,
        )
