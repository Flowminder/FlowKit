# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import Schema, fields, post_load
from marshmallow.validate import OneOf

from flowmachine.features import RadiusOfGyration
from .base_exposed_query import BaseExposedQuery
from .custom_fields import SubscriberSubset

__all__ = ["RadiusOfGyrationSchema", "RadiusOfGyrationExposed"]


class RadiusOfGyrationSchema(Schema):
    query_kind = fields.String(validate=OneOf(["radius_of_gyration"]))
    start_date = fields.Date(required=True)
    end_date = fields.Date(required=True)
    subscriber_subset = SubscriberSubset()

    @post_load
    def make_query_object(self, params):
        return RadiusOfGyrationExposed(**params)


class RadiusOfGyrationExposed(BaseExposedQuery):
    def __init__(self, *, start_date, end_date, subscriber_subset=None):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.start_date = start_date
        self.end_date = end_date
        self.subscriber_subset = subscriber_subset

    @property
    def _flowmachine_query_obj(self):
        """
        Return the underlying flowmachine radius_of_gyration object.

        Returns
        -------
        Query
        """
        return RadiusOfGyration(
            start=self.start_date,
            stop=self.end_date,
            subscriber_subset=self.subscriber_subset,
        )
