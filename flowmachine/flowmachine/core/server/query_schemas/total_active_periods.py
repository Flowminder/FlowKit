# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import fields
from marshmallow.validate import OneOf

from flowmachine.features import TotalActivePeriodsSubscriber
from .base_query_with_sampling import BaseExposedQueryWithSampling
from .base_schema import BaseSchema
from .field_mixins import (
    EventTypesField,
    SubscriberSubsetField,
    HoursField,
)
from .custom_fields import ISODateTime

__all__ = ["TotalActivePeriodsSchema", "TotalActivePeriodsExposed"]


class TotalActivePeriodsExposed(BaseExposedQueryWithSampling):
    # query_kind class attribute is required for nesting and serialisation
    query_kind = "total_active_periods"

    def __init__(
        self,
        *,
        start_date,
        total_periods,
        event_types,
        period_unit="days",
        period_length=1,
        subscriber_subset=None,
        sampling=None,
        hours=None
    ):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.start = start_date
        self.total_periods = total_periods
        self.period_unit = period_unit
        self.period_length = period_length
        self.subscriber_subset = subscriber_subset
        self.sampling = sampling
        self.hours = hours
        self.event_types = event_types

    @property
    def _unsampled_query_obj(self):
        """
        Return the underlying flowmachine object.

        Returns
        -------
        Query
        """
        return TotalActivePeriodsSubscriber(
            start=self.start,
            total_periods=self.total_periods,
            period_unit=self.period_unit,
            period_length=self.period_length,
            subscriber_subset=self.subscriber_subset,
            hours=self.hours,
            table=self.event_types,
        )


class TotalActivePeriodsSchema(
    EventTypesField, SubscriberSubsetField, HoursField, BaseSchema
):
    __model__ = TotalActivePeriodsExposed

    # query_kind parameter is required here for claims validation
    query_kind = fields.String(validate=OneOf([__model__.query_kind]), required=True)
    start_date = ISODateTime(required=True)
    total_periods = fields.Integer(required=True)
    period_length = fields.Integer(missing=1, required=False, default=1)
    period_unit = fields.String(
        validate=OneOf(TotalActivePeriodsSubscriber.allowed_units),
        missing="days",
        required=False,
        default="days",
    )
