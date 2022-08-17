# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import fields
from marshmallow.validate import OneOf

from flowmachine.features import daily_location
from .custom_fields import ISODateTime
from .aggregation_unit import AggregationUnitMixin
from .base_query_with_sampling import (
    BaseQueryWithSamplingSchema,
    BaseExposedQueryWithSampling,
)
from .field_mixins import HoursField, EventTypesField, SubscriberSubsetField

__all__ = ["DailyLocationSchema", "DailyLocationExposed"]


class DailyLocationExposed(BaseExposedQueryWithSampling):
    # query_kind class attribute is required for nesting and serialisation
    query_kind = "daily_location"

    def __init__(
        self,
        date,
        *,
        method,
        aggregation_unit,
        event_types,
        subscriber_subset=None,
        sampling=None,
        hours=None
    ):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.date = date
        self.method = method
        self.aggregation_unit = aggregation_unit
        self.event_types = event_types
        self.subscriber_subset = subscriber_subset
        self.sampling = sampling
        self.hours = hours

    @property
    def _unsampled_query_obj(self):
        """
        Return the underlying flowmachine daily_location object.

        Returns
        -------
        Query
        """
        return daily_location(
            date=self.date,
            spatial_unit=self.aggregation_unit,
            method=self.method,
            table=self.event_types,
            subscriber_subset=self.subscriber_subset,
            hours=self.hours,
        )


class DailyLocationSchema(
    EventTypesField,
    SubscriberSubsetField,
    HoursField,
    AggregationUnitMixin,
    BaseQueryWithSamplingSchema,
):
    __model__ = DailyLocationExposed

    # query_kind parameter is required here for claims validation
    query_kind = fields.String(validate=OneOf([__model__.query_kind]), required=True)
    date = ISODateTime(required=True)
    method = fields.String(required=True, validate=OneOf(["last", "most-common"]))
