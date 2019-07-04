# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import Schema, fields, post_load
from marshmallow.validate import OneOf, Length

from flowmachine.features import NocturnalEvents
from .base_exposed_query import BaseExposedQuery
from .custom_fields import SubscriberSubset

__all__ = ["NocturnalEventsSchema", "NocturnalEventsExposed"]


class NocturnalEventsSchema(Schema):
    query_kind = fields.String(validate=OneOf(["nocturnal_events"]))
    start = fields.Date(required=True)
    stop = fields.Date(required=True)
    hours = fields.Tuple((fields.Integer(), fields.Integer()))
    subscriber_subset = SubscriberSubset()

    @post_load
    def make_query_object(self, params, **kwargs):
        return NocturnalEventsExposed(**params)


class NocturnalEventsExposed(BaseExposedQuery):
    def __init__(self, *, start, stop, hours, subscriber_subset=None):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.start = start
        self.stop = stop
        self.hours = hours
        self.subscriber_subset = subscriber_subset

    @property
    def _flowmachine_query_obj(self):
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
