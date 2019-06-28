# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import Schema, fields, post_load
from marshmallow.validate import OneOf

from flowmachine.features.dfs import DFSTotalMetricAmount
from .base_exposed_query import BaseExposedQuery
from .custom_fields import DFSMetric
from .aggregation_unit import AggregationUnit

__all__ = ["DFSTotalMetricAmountSchema", "DFSTotalMetricAmountExposed"]


class DFSTotalMetricAmountSchema(Schema):
    # query_kind parameter is required here for claims validation
    query_kind = fields.String(validate=OneOf(["dfs_metric_total_amount"]))
    metric = DFSMetric()
    start_date = fields.Date(required=True)
    end_date = fields.Date(required=True)
    aggregation_unit = AggregationUnit()

    @post_load
    def make_query_object(self, params, **kwargs):
        return DFSTotalMetricAmountExposed(**params)


class DFSTotalMetricAmountExposed(BaseExposedQuery):
    def __init__(self, *, metric, start_date, end_date, aggregation_unit):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.metric = metric
        self.start_date = start_date
        self.end_date = end_date
        self.aggregation_unit = aggregation_unit

    @property
    def _flowmachine_query_obj(self):
        """
        Return the underlying flowmachine daily_location object.

        Returns
        -------
        Query
        """
        return DFSTotalMetricAmount(
            metric=self.metric,
            start_date=self.start_date,
            end_date=self.end_date,
            aggregation_unit=self.aggregation_unit,
        )
