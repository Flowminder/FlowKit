# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import Schema, fields, post_load
from marshmallow.validate import OneOf, Length

from flowmachine.features import UniqueSubscriberCounts
from flowmachine.features.location.redacted_unique_subscriber_counts import (
    RedactedUniqueSubscriberCounts,
)
from .base_exposed_query import BaseExposedQuery
from .aggregation_unit import AggregationUnit, get_spatial_unit_obj

__all__ = ["UniqueSubscriberCountsSchema", "UniqueSubscriberCountsExposed"]


class UniqueSubscriberCountsSchema(Schema):
    # query_kind parameter is required here for claims validation
    query_kind = fields.String(validate=OneOf(["unique_subscriber_counts"]))
    start_date = fields.Date(required=True)
    end_date = fields.Date(required=True)
    aggregation_unit = AggregationUnit()

    @post_load
    def make_query_object(self, params, **kwargs):
        return UniqueSubscriberCountsExposed(**params)


class UniqueSubscriberCountsExposed(BaseExposedQuery):
    def __init__(self, *, start_date, end_date, aggregation_unit):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.start_date = start_date
        self.end_date = end_date
        self.aggregation_unit = aggregation_unit

    @property
    def _flowmachine_query_obj(self):
        """
        Return the underlying flowmachine UniqueSubscriberCounts object.

        Returns
        -------
        Query
        """
        return RedactedUniqueSubscriberCounts(
            unique_subscriber_counts=UniqueSubscriberCounts(
                start=self.start_date,
                stop=self.end_date,
                spatial_unit=get_spatial_unit_obj(self.aggregation_unit),
            )
        )
