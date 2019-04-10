# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import Schema, fields, post_load
from marshmallow.validate import OneOf, Length

from flowmachine.features import TotalNetworkObjects
from flowmachine.features.network.total_network_objects import valid_periods
from .base_exposed_query import BaseExposedQuery
from .custom_fields import AggregationUnit

__all__ = ["TotalNetworkObjectsSchema", "TotalNetworkObjectsExposed"]


class TotalNetworkObjectsSchema(Schema):

    start_date = fields.Date(required=True)
    end_date = fields.Date(required=True)
    period = fields.String(default="day", validate=OneOf(valid_periods))
    aggregation_unit = AggregationUnit()

    @post_load
    def make_query_object(self, params):
        return TotalNetworkObjectsExposed(**params)


class TotalNetworkObjectsExposed(BaseExposedQuery):
    def __init__(self, *, start_date, end_date, aggregation_unit, period):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.start_date = start_date
        self.end_date = end_date
        self.aggregation_unit = aggregation_unit
        self.period = period

    @property
    def _flowmachine_query_obj(self):
        """
        Return the underlying flowmachine daily_location object.

        Returns
        -------
        Query
        """
        return TotalNetworkObjects(
            start=self.start_date,
            stop=self.end_date,
            level=self.aggregation_unit,
            period=self.period,
        )
