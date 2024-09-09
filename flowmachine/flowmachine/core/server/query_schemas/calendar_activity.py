# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import fields
from marshmallow.validate import OneOf

from flowmachine.features.nonspatial_aggregates.redacted_per_value_aggregate import (
    RedactedPerValueAggregate,
)

from flowmachine.features.subscriber.calendar_activity import CalendarActivity

from .base_query_with_sampling import (
    BaseQueryWithSamplingSchema,
    BaseExposedQueryWithSampling,
)
from .custom_fields import ISODateTime
from .field_mixins import (
    HoursField,
    EventTypesField,
    SubscriberSubsetField,
)

__all__ = ["CalendarActivitySchema", "CalendarActivityExposed"]


class CalendarActivityExposed(BaseExposedQueryWithSampling):
    # query_kind class attribute is required for nesting and serialisation
    query_kind = "calendar_activity"

    def __init__(
        self,
        start_date,
        total_periods,
        *,
        event_types,
        period_length=1,
        period_unit="days",
        subscriber_subset=None,
        sampling=None,
        hours=None,
    ):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.start_date = start_date
        self.total_periods = total_periods
        self.period_length = period_length
        self.period_unit = period_unit
        self.event_types = event_types
        self.subscriber_subset = subscriber_subset
        self.sampling = sampling
        self.hours = hours

    @property
    def _unsampled_query_obj(self) -> RedactedPerValueAggregate:
        """
        Return the underlying flowmachine calendar activity object.

        Returns
        -------
        RedactedPerValueAggregate
        """
        calendar_query = CalendarActivity(
            self.start_date,
            self.total_periods,
            period_length=self.period_length,
            period_unit=self.period_unit,
            table=self.event_types,
            subscriber_subset=self.subscriber_subset,
            hours=self.hours,
        )
        return RedactedPerValueAggregate(calendar_query)


class CalendarActivitySchema(
    EventTypesField,
    SubscriberSubsetField,
    HoursField,
    BaseQueryWithSamplingSchema,
):
    __model__ = CalendarActivityExposed

    query_kind = fields.String(validate=OneOf([__model__.query_kind]), required=True)
    start_date = ISODateTime(required=True)
    total_periods = fields.Integer(required=True)
    period_length = fields.Integer(missing=1, required=False, default=1)
    period_unit = fields.String(
        validate=OneOf(CalendarActivity.allowed_units),
        missing="days",
        required=False,
        default="days",
    )
