# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import fields, pre_load, ValidationError
from marshmallow.validate import OneOf
from marshmallow_oneofschema import OneOfSchema

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
from flowmachine.core.server.query_schemas.spatial_aggregate import (
    InputToSpatialAggregate,
)
from flowmachine.features.location.joined_spatial_aggregate import (
    JoinedSpatialAggregate,
)
from flowmachine.features.location.redacted_joined_spatial_aggregate import (
    RedactedJoinedSpatialAggregate,
)
from .base_exposed_query import BaseExposedQuery


__all__ = ["JoinedSpatialAggregateSchema", "JoinedSpatialAggregateExposed"]

from .base_schema import BaseSchema


class JoinableMetrics(OneOfSchema):
    type_field = "query_kind"
    type_schemas = {
        "radius_of_gyration": RadiusOfGyrationSchema,
        "unique_location_counts": UniqueLocationCountsSchema,
        "topup_balance": TopUpBalanceSchema,
        "subscriber_degree": SubscriberDegreeSchema,
        "topup_amount": TopUpAmountSchema,
        "event_count": EventCountSchema,
        "handset": HandsetSchema,
        "pareto_interactions": ParetoInteractionsSchema,
        "nocturnal_events": NocturnalEventsSchema,
        "displacement": DisplacementSchema,
    }


class JoinedSpatialAggregateExposed(BaseExposedQuery):
    def __init__(self, *, locations, metric, method):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.locations = locations
        self.metric = metric
        self.method = method

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
    # query_kind parameter is required here for claims validation
    query_kind = fields.String(validate=OneOf(["joined_spatial_aggregate"]))
    locations = fields.Nested(InputToSpatialAggregate, required=True)
    metric = fields.Nested(JoinableMetrics, required=True)
    method = fields.String(validate=OneOf(JoinedSpatialAggregate.allowed_methods))

    @pre_load
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
        ]
        categorical_metrics = ["handset"]
        if data["metric"]["query_kind"] in continuous_metrics:
            validate = OneOf(
                ["avg", "max", "min", "median", "mode", "stddev", "variance"]
            )
        elif data["metric"]["query_kind"] in categorical_metrics:
            validate = OneOf(["distr"])
        else:
            raise ValidationError(
                f"{data['metric']['query_kind']} does not have a valid metric type."
            )
        validate(data["method"])
        return data

    __model__ = JoinedSpatialAggregateExposed
