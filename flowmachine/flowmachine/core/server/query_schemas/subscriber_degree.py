# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import Schema, fields, post_load
from marshmallow.validate import OneOf, Length

from flowmachine.features import SubscriberDegree
from .base_exposed_query import BaseExposedQuery
from .custom_fields import SubscriberSubset

__all__ = ["SubscriberDegreeSchema", "SubscriberDegreeExposed"]


class SubscriberDegreeSchema(Schema):
    query_kind = fields.String(validate=OneOf(["subscriber_degree"]))
    start_date = fields.Date(required=True)
    end_date = fields.Date(required=True)
    direction = fields.String(
        required=False, validate=OneOf(["in", "out", "both"]), default="both"
    )  # TODO: use a globally defined enum for this
    subscriber_subset = SubscriberSubset()

    @post_load
    def make_query_object(self, params):
        return SubscriberDegreeExposed(**params)


class SubscriberDegreeExposed(BaseExposedQuery):
    def __init__(self, *, start_date, end_date, direction, subscriber_subset=None):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.start_date = start_date
        self.end_date = end_date
        self.direction = direction
        self.subscriber_subset = subscriber_subset

    @property
    def _flowmachine_query_obj(self):
        """
        Return the underlying flowmachine subscriber_degree object.

        Returns
        -------
        Query
        """
        return SubscriberDegree(
            start=self.start_date,
            stop=self.end_date,
            direction=self.direction,
            subscriber_subset=self.subscriber_subset,
        )
