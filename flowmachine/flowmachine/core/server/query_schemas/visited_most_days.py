# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import fields
from marshmallow.validate import OneOf

from .base_query_with_sampling import (
    BaseQueryWithSamplingSchema,
    BaseExposedQueryWithSampling,
)
from .field_mixins import (
    HoursField,
    StartAndEndField,
    EventTypesField,
    SubscriberSubsetField,
)
from .aggregation_unit import AggregationUnitMixin
from flowmachine.features.utilities.subscriber_locations import SubscriberLocations
from flowmachine.features.subscriber.visited_most_days import VisitedMostDays


__all__ = [
    "VisitedMostDaysSchema",
    "VisitedMostDaysExposed",
]


class VisitedMostDaysExposed(BaseExposedQueryWithSampling):
    # query_kind class attribute is required for nesting and serialisation
    query_kind = "visited_most_days"

    def __init__(
        self,
        start_date,
        end_date,
        *,
        aggregation_unit,
        event_types,
        subscriber_subset=None,
        hours=None,
        sampling=None,
    ):
        self.start_date = start_date
        self.end_date = end_date
        self.aggregation_unit = aggregation_unit
        self.hours = hours
        self.event_types = event_types
        self.subscriber_subset = subscriber_subset
        self.sampling = sampling

    @property
    def _unsampled_query_obj(self):
        """
        Return the underlying flowmachine visited_most days object.

        Returns
        -------
        Query
        """
        return VisitedMostDays(
            subscriber_locations=SubscriberLocations(
                start=self.start_date,
                stop=self.end_date,
                hours=self.hours,
                spatial_unit=self.aggregation_unit,
                table=self.event_types,
                subscriber_subset=self.subscriber_subset,
            )
        )


class VisitedMostDaysSchema(
    StartAndEndField,
    EventTypesField,
    SubscriberSubsetField,
    HoursField,
    AggregationUnitMixin,
    BaseQueryWithSamplingSchema,
):
    __model__ = VisitedMostDaysExposed

    # query_kind parameter is required here for claims validation
    query_kind = fields.String(validate=OneOf([__model__.query_kind]), required=True)
