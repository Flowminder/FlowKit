# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from functools import reduce

from marshmallow import fields
from marshmallow.validate import OneOf

from flowmachine.core.server.query_schemas import BaseExposedQuery
from flowmachine.core.server.query_schemas.subscriber_subset import SubscriberSubset
from flowmachine.core.server.query_schemas.base_schema import BaseSchema
from flowmachine.features.subscriber.per_subscriber_aggregate import (
    PerSubscriberAggregate,
    agg_methods,
)


class PerSubscriberAggregateExposed(BaseExposedQuery):
    def __init__(self, subscriber_queries, agg_method):
        self.subscriber_query = reduce(lambda x, y: x.join(y), subscriber_queries)
        self.agg_method = agg_method

    def _flomachine_query_obj(self):
        return PerSubscriberAggregate(
            subscriber_query=self.subscriber_query,
            agg_column="value",
            agg_method=self.agg_method,
        )


class PerSubscriberAggregateSchema(BaseSchema):
    query_kind = fields.String(validate=OneOf(["per_subscriber_aggregate"]))
    # TODO: Can we make this a list or a single query?
    subscriber_query = fields.List(SubscriberSubset)
    agg_method = fields.String(validate=OneOf(agg_methods))

    __model__ = PerSubscriberAggregateExposed
