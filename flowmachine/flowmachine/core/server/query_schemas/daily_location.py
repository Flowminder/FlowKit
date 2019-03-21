# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import Schema, fields, post_load
from marshmallow.validate import OneOf

from flowmachine.features import daily_location
from .base_exposed_query import BaseExposedQuery

__all__ = ["DailyLocationSchema", "DailyLocationExposed"]


class DailyLocationSchema(Schema):
    date = fields.Date(required=True)
    method = fields.String(required=True, validate=OneOf(["last", "most-common"]))
    aggregation_unit = fields.String(
        required=True, validate=OneOf(["admin0", "admin1", "admin2", "admin3"])
    )
    subscriber_subset = fields.String(
        required=False, allow_none=True, validate=OneOf([None])
    )

    @post_load
    def make_query_object(self, params):
        return DailyLocationExposed(**params)


class DailyLocationExposed(BaseExposedQuery):

    __schema__ = DailyLocationSchema

    def __init__(self, date, *, method, aggregation_unit, subscriber_subset=None):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.date = date
        self.method = method
        self.aggregation_unit = aggregation_unit
        self.subscriber_subset = subscriber_subset

    @property
    def _flowmachine_query_obj(self):
        """
        Return the underlying flowmachine daily_location object.

        Returns
        -------
        Query
        """
        return daily_location(
            date=self.date,
            level=self.aggregation_unit,
            method=self.method,
            subscriber_subset=self.subscriber_subset,
        )
