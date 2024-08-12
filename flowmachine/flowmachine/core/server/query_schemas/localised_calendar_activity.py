# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import fields
from marshmallow.validate import OneOf

from flowmachine.features import TotalActivePeriodsSubscriber
from flowmachine.features.location.labelled_spatial_aggregate import (
    LabelledSpatialAggregate,
)
from flowmachine.features.location.redacted_labelled_spatial_aggregate import (
    RedactedLabelledSpatialAggregate,
)
from flowmachine.features.nonspatial_aggregates.redacted_per_value_aggregate import (
    RedactedPerValueAggregate,
)
from flowmachine.features.subscriber.calendar_activity import CalendarActivity
from .aggregation_unit import AggregationUnitMixin, AggregationUnitKind
from .base_query_with_sampling import (
    BaseQueryWithSamplingSchema,
    BaseExposedQueryWithSampling,
)
from .calendar_activity import CalendarActivitySchema
from .custom_fields import ISODateTime
from .field_mixins import (
    HoursField,
    StartAndEndField,
    EventTypesField,
    SubscriberSubsetField,
)

__all__ = ["LocalisedCalendarActivitySchema", "LocalisedCalendarActivityExposed"]

from .reference_location import ReferenceLocationSchema


class LocalisedCalendarActivityExposed(BaseExposedQueryWithSampling):
    # query_kind class attribute is required for nesting and serialisation
    query_kind = "localised_calendar_activity"

    def __init__(
        self,
        start_date,
        total_periods,
        reference_location,
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
        self.reference_location = reference_location

    @property
    def aggregation_unit(self):
        return self.reference_location.aggregation_unit

    @property
    def _unsampled_query_obj(self) -> RedactedLabelledSpatialAggregate:
        """
        Return the underlying flowmachine calendar activity object.

        Returns
        -------
        RedactedLabelledSpatialAggregate
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
        return RedactedLabelledSpatialAggregate(
            labelled_spatial_aggregate=LabelledSpatialAggregate(
                locations=self.reference_location._flowmachine_query_obj,
                labels=calendar_query,
            )
        )


class LocalisedCalendarActivitySchema(CalendarActivitySchema):
    __model__ = LocalisedCalendarActivityExposed

    query_kind = fields.String(validate=OneOf([__model__.query_kind]), required=True)
    aggregation_unit = AggregationUnitKind(dump_only=True)
    reference_location = fields.Nested(ReferenceLocationSchema, required=False)
