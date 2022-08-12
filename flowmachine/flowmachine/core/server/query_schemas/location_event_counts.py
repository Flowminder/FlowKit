# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import fields
from marshmallow.validate import OneOf

from flowmachine.features import TotalLocationEvents
from flowmachine.features.location.redacted_total_events import RedactedTotalEvents
from .base_exposed_query import BaseExposedQuery
from .custom_fields import Direction
from .field_mixins import (
    HoursField,
    StartAndEndField,
    EventTypesField,
    SubscriberSubsetField,
)
from .base_schema import BaseSchema
from .aggregation_unit import AggregationUnitMixin

__all__ = ["LocationEventCountsSchema", "LocationEventCountsExposed"]


class LocationEventCountsExposed(BaseExposedQuery):
    # query_kind class attribute is required for nesting and serialisation
    query_kind = "location_event_counts"

    def __init__(
        self,
        *,
        start_date,
        end_date,
        interval,
        direction,
        event_types,
        aggregation_unit,
        subscriber_subset=None,
        hours=None
    ):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.start_date = start_date
        self.end_date = end_date
        self.interval = interval
        self.direction = direction
        self.event_types = event_types
        self.aggregation_unit = aggregation_unit
        self.subscriber_subset = subscriber_subset
        self.hours = hours

    @property
    def _flowmachine_query_obj(self):
        """
        Return the underlying flowmachine daily_location object.

        Returns
        -------
        Query
        """
        return RedactedTotalEvents(
            total_events=TotalLocationEvents(
                start=self.start_date,
                stop=self.end_date,
                interval=self.interval,
                direction=self.direction,
                table=self.event_types,
                spatial_unit=self.aggregation_unit,
                subscriber_subset=self.subscriber_subset,
                hours=self.hours,
            )
        )


class LocationEventCountsSchema(
    StartAndEndField,
    EventTypesField,
    SubscriberSubsetField,
    HoursField,
    AggregationUnitMixin,
    BaseSchema,
):
    __model__ = LocationEventCountsExposed

    # query_kind parameter is required here for claims validation
    query_kind = fields.String(validate=OneOf([__model__.query_kind]), required=True)
    interval = fields.String(
        required=True, validate=OneOf(TotalLocationEvents.allowed_intervals)
    )
    direction = Direction()
