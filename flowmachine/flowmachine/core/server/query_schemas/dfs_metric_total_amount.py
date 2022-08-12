# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import fields
from marshmallow.validate import OneOf

from flowmachine.features.dfs import DFSTotalMetricAmount
from .base_exposed_query import BaseExposedQuery
from .field_mixins import StartAndEndField
from .base_schema import BaseSchema
from .custom_fields import DFSMetric
from .aggregation_unit import AggregationUnitKind

__all__ = ["DFSTotalMetricAmountSchema", "DFSTotalMetricAmountExposed"]

from ...spatial_unit import AdminSpatialUnit


class DFSTotalMetricAmountExposed(BaseExposedQuery):
    # query_kind class attribute is required for nesting and serialisation
    query_kind = "dfs_metric_total_amount"

    def __init__(
        self, *, metric, start_date, end_date, aggregation_unit: AdminSpatialUnit
    ):
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
            aggregation_unit=self.aggregation_unit.canonical_name,
        )


class DFSTotalMetricAmountSchema(StartAndEndField, BaseSchema):
    __model__ = DFSTotalMetricAmountExposed

    # query_kind parameter is required here for claims validation
    query_kind = fields.String(validate=OneOf([__model__.query_kind]), required=True)
    metric = DFSMetric()
    aggregation_unit = AggregationUnitKind(required=True)
