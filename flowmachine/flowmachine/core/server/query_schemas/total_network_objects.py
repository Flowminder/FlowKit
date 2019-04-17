# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import Schema, fields, post_load
from marshmallow.validate import OneOf, Length

from flowmachine.features import TotalNetworkObjects
from flowmachine.features.network.total_network_objects import valid_periods
from .base_exposed_query import BaseExposedQuery
from .custom_fields import AggregationUnit, TotalBy

__all__ = ["TotalNetworkObjectsSchema", "TotalNetworkObjectsExposed"]


class TotalNetworkObjectsSchema(Schema):
    query_kind = fields.String(validate=OneOf(["total_network_objects"]))
    start_date = fields.Date(required=True)
    end_date = fields.Date(required=True)
    total_by = TotalBy(required=False, missing="day")
    aggregation_unit = AggregationUnit()

    @post_load
    def make_query_object(self, params):
        return TotalNetworkObjectsExposed(**params)


class TotalNetworkObjectsExposed(BaseExposedQuery):
    def __init__(self, *, start_date, end_date, aggregation_unit, total_by):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.start_date = start_date
        self.end_date = end_date
        self.aggregation_unit = aggregation_unit
        self.total_by = total_by

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
            total_by=self.total_by,
        )
