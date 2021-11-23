# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from functools import reduce

from marshmallow import fields
from marshmallow.validate import OneOf, Length

from flowmachine.core.server.query_schemas import BaseExposedQuery
from flowmachine.core.server.query_schemas.numeric_subscriber_metrics import (
    NumericSubscriberMetricsSchema,
)
from flowmachine.core.server.query_schemas.base_schema import BaseSchema
from flowmachine.features.subscriber.per_subscriber_aggregate import (
    PerSubscriberAggregate,
    agg_methods,
)


class PerSubscriberAggregateExposed(BaseExposedQuery):
    def __init__(self, subscriber_queries, agg_method):
        self.subscriber_queries = subscriber_queries
        self.agg_method = agg_method

    @property
    def _flowmachine_query_obj(self):
        subscriber_query = reduce(
            # TODO: Replace with Jono's new list input to union
            lambda x, y: x._flowmachine_query_obj.union(y._flowmachine_query_obj),
            self.subscriber_queries,
        )
        return PerSubscriberAggregate(
            subscriber_query=subscriber_query,
            agg_column="value",
            agg_method=self.agg_method,
        )


class PerSubscriberAggregateSchema(BaseSchema):
    query_kind = fields.String(validate=OneOf(["per_subscriber_aggregate"]))
    subscriber_queries = fields.List(
        fields.Nested(NumericSubscriberMetricsSchema), validate=Length(min=1)
    )
    agg_method = fields.String(validate=OneOf(agg_methods))

    __model__ = PerSubscriberAggregateExposed
