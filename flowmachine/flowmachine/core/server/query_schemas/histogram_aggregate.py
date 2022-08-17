# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import Schema, fields, post_load, validates_schema, ValidationError
from marshmallow.validate import OneOf

from flowmachine.core.server.query_schemas.custom_fields import Bounds
from flowmachine.core.server.query_schemas.radius_of_gyration import (
    RadiusOfGyrationSchema,
)
from flowmachine.core.server.query_schemas.subscriber_degree import (
    SubscriberDegreeSchema,
)
from flowmachine.core.server.query_schemas.topup_amount import TopUpAmountSchema
from flowmachine.core.server.query_schemas.event_count import EventCountSchema
from flowmachine.core.server.query_schemas.nocturnal_events import NocturnalEventsSchema
from flowmachine.core.server.query_schemas.unique_location_counts import (
    UniqueLocationCountsSchema,
)
from flowmachine.core.server.query_schemas.displacement import DisplacementSchema
from flowmachine.core.server.query_schemas.pareto_interactions import (
    ParetoInteractionsSchema,
)
from flowmachine.core.server.query_schemas.topup_balance import TopUpBalanceSchema

from flowmachine.features import HistogramAggregation
from .base_exposed_query import BaseExposedQuery


__all__ = ["HistogramAggregateSchema", "HistogramAggregateExposed"]

from .base_schema import BaseSchema
from .total_active_periods import TotalActivePeriodsSchema
from .one_of_query import OneOfQuerySchema


class HistogrammableMetrics(OneOfQuerySchema):
    query_schemas = (
        RadiusOfGyrationSchema,
        UniqueLocationCountsSchema,
        TopUpBalanceSchema,
        SubscriberDegreeSchema,
        TopUpAmountSchema,
        EventCountSchema,
        ParetoInteractionsSchema,
        NocturnalEventsSchema,
        DisplacementSchema,
        TotalActivePeriodsSchema,
    )


class HistogramBins(Schema):
    """
    Schema representing a range (i.e. lower and upper bound, both required, lower bound must be less than upper.
    """

    n_bins = fields.Integer()
    bin_list = fields.List(fields.Float)

    @validates_schema
    def validate_one_of_bins_or_list(self, data, **kwargs):
        if "n_bins" in data and "bin_list" in data:
            raise ValidationError("Only one of n_bins or bin_list may be provided.")

    @validates_schema
    def validate_at_least_one_of_bins_or_list(self, data, **kwargs):
        if "n_bins" in data or "bin_list" in data:
            return
        raise ValidationError("One of n_bins or bin_list must be provided.")

    @post_load
    def to_value(self, params, **kwargs):
        return params.get("n_bins", params.get("bin_list"))


class HistogramAggregateExposed(BaseExposedQuery):
    # query_kind class attribute is required for nesting and serialisation
    query_kind = "histogram_aggregate"

    def __init__(self, *, metric, bins, range=None):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.metric = metric
        self.bins = bins
        self.range = range if range is None else tuple(range)

    @property
    def _flowmachine_query_obj(self):
        """
        Return the underlying flowmachine object.

        Returns
        -------
        Query
        """
        metric = self.metric._flowmachine_query_obj
        return HistogramAggregation(metric=metric, bins=self.bins, range=self.range)


class HistogramAggregateSchema(BaseSchema):
    __model__ = HistogramAggregateExposed

    # query_kind parameter is required here for claims validation
    query_kind = fields.String(validate=OneOf([__model__.query_kind]), required=True)
    metric = fields.Nested(HistogrammableMetrics, required=True)
    range = fields.Nested(Bounds)
    bins = fields.Nested(HistogramBins, required=True)
