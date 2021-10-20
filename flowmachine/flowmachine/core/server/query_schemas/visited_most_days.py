# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import fields
from marshmallow.validate import OneOf

from .base_schema import BaseSchema
from .base_exposed_query import BaseExposedQuery
from .field_mixins import StartAndEndField
from .aggregation_unit import AggregationUnitMixin
from flowmachine.features.subscriber.visited_most_days import VisitedMostDays


__all__ = [
    "VisitedMostDaysSchema",
    "VisitedMostDaysExposed",
]


class VisitedMostDaysExposed(BaseExposedQuery):
    def __init__(
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self,
        start_date,
        end_date,
        aggregation_unit,
    ):
        self.start_date = start_date
        self.end_date = end_date
        self.aggregation_unit = aggregation_unit

    @property
    def _flowmachine_query_obj(self):
        return VisitedMostDays(
            start_date=self.start_date,
            end_date=self.end_date,
            spatial_unit=self.aggregation_unit,
        )


class VisitedMostDaysSchema(StartAndEndField, AggregationUnitMixin, BaseSchema):
    query_kind = fields.String(validate=OneOf(["visited_most_days"]))

    __model__ = VisitedMostDaysExposed
