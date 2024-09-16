# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import fields, validates_schema, ValidationError
from marshmallow.validate import OneOf

from flowmachine.core.server.query_schemas.radius_of_gyration import (
    RadiusOfGyrationSchema,
)
from flowmachine.core.server.query_schemas.subscriber_degree import (
    SubscriberDegreeSchema,
)
from flowmachine.core.server.query_schemas.topup_amount import TopUpAmountSchema
from flowmachine.core.server.query_schemas.event_count import EventCountSchema
from flowmachine.core.server.query_schemas.handset import HandsetSchema
from flowmachine.core.server.query_schemas.nocturnal_events import NocturnalEventsSchema
from flowmachine.core.server.query_schemas.unique_location_counts import (
    UniqueLocationCountsSchema,
)
from flowmachine.core.server.query_schemas.displacement import DisplacementSchema
from flowmachine.core.server.query_schemas.pareto_interactions import (
    ParetoInteractionsSchema,
)
from flowmachine.core.server.query_schemas.topup_balance import TopUpBalanceSchema

from flowmachine.features.location.joined_spatial_aggregate import (
    JoinedSpatialAggregate,
)
from flowmachine.features.location.redacted_joined_spatial_aggregate import (
    RedactedJoinedSpatialAggregate,
)
from ...statistic_types import Statistic
from .base_exposed_query import BaseExposedQuery
from .aggregation_unit import AggregationUnitKind


__all__ = ["JoinedSpatialAggregateSchema", "JoinedSpatialAggregateExposed"]

from .base_schema import BaseSchema
from .one_of_query import OneOfQuerySchema
from .reference_location import ReferenceLocationSchema
from .total_active_periods import TotalActivePeriodsSchema


class JoinableMetrics(OneOfQuerySchema):
    query_schemas = (
        RadiusOfGyrationSchema,
        UniqueLocationCountsSchema,
        TopUpBalanceSchema,
        SubscriberDegreeSchema,
        TopUpAmountSchema,
        EventCountSchema,
        HandsetSchema,
        ParetoInteractionsSchema,
        NocturnalEventsSchema,
        DisplacementSchema,
        TotalActivePeriodsSchema,
    )


class JoinedSpatialAggregateExposed(BaseExposedQuery):
    # query_kind class attribute is required for nesting and serialisation
    query_kind = "joined_spatial_aggregate"

    def __init__(self, *, locations, metric, method):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.locations = locations
        self.metric = metric
        self.method = method

    @property
    def aggregation_unit(self):
        return self.locations.aggregation_unit

    @property
    def _flowmachine_query_obj(self):
        """
        Return the underlying flowmachine object.

        Returns
        -------
        Query
        """
        locations = self.locations._flowmachine_query_obj
        metric = self.metric._flowmachine_query_obj
        return RedactedJoinedSpatialAggregate(
            joined_spatial_aggregate=JoinedSpatialAggregate(
                locations=locations, metric=metric, method=self.method
            )
        )


class JoinedSpatialAggregateSchema(BaseSchema):
    __model__ = JoinedSpatialAggregateExposed

    # query_kind parameter is required here for claims validation
    query_kind = fields.String(validate=OneOf([__model__.query_kind]), required=True)
    aggregation_unit = AggregationUnitKind(dump_only=True)
    locations = fields.Nested(ReferenceLocationSchema, required=True)
    metric = fields.Nested(JoinableMetrics, required=True)
    method = fields.String(
        validate=OneOf(JoinedSpatialAggregate.allowed_methods), required=True
    )

    @validates_schema(skip_on_field_errors=True)
    def validate_method(self, data, **kwargs):
        continuous_metrics = [
            "radius_of_gyration",
            "unique_location_counts",
            "topup_balance",
            "subscriber_degree",
            "topup_amount",
            "event_count",
            "nocturnal_events",
            "pareto_interactions",
            "displacement",
            "total_active_periods",
        ]
        categorical_metrics = ["handset"]
        if data["metric"].query_kind in continuous_metrics:
            validate = OneOf([f"{stat}" for stat in Statistic])
        elif data["metric"].query_kind in categorical_metrics:
            validate = OneOf(["distr", "mode"])
        else:
            raise ValidationError(
                f"{data['metric'].query_kind} does not have a valid metric type."
            )
        validate(data["method"])
