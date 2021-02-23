# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import fields, validate
from marshmallow.validate import OneOf

from flowmachine.features.subscriber.most_frequent_location import MostFrequentLocation
from .custom_fields import EventTypes, ISODateTime
from .subscriber_subset import SubscriberSubset
from .aggregation_unit import AggregationUnitMixin
from .base_query_with_sampling import (
    BaseQueryWithSamplingSchema,
    BaseExposedQueryWithSampling,
)

__all__ = ["MostFrequentLocationSchema", "MostFrequentLocationExposed"]


class MostFrequentLocationExposed(BaseExposedQueryWithSampling):
    def __init__(
        self,
        start,
        stop,
        *,
        aggregation_unit,
        event_types,
        subscriber_subset=None,
        sampling=None,
        start_hour=None,
        end_hour=None
    ):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.start = start
        self.stop = stop
        self.hours = (
            None if start_hour is None or end_hour is None else (start_hour, end_hour)
        )
        self.aggregation_unit = aggregation_unit
        self.event_types = event_types
        self.subscriber_subset = subscriber_subset
        self.sampling = sampling

    @property
    def _unsampled_query_obj(self):
        """
        Return the underlying flowmachine daily_location object.

        Returns
        -------
        Query
        """
        return MostFrequentLocation(
            start=self.start,
            stop=self.stop,
            hours=self.hours,
            spatial_unit=self.aggregation_unit,
            table=self.event_types,
            subscriber_subset=self.subscriber_subset,
        )


class MostFrequentLocationSchema(AggregationUnitMixin, BaseQueryWithSamplingSchema):
    # query_kind parameter is required here for claims validation
    query_kind = fields.String(validate=OneOf(["most_frequent_location"]))
    start = ISODateTime(required=True)
    stop = ISODateTime(required=True)
    start_hour = fields.Integer(
        validate=validate.Range(min=0, max=24, min_inclusive=True, max_inclusive=True),
        required=False,
        default=None,
    )
    end_hour = fields.Integer(
        validate=validate.Range(min=0, max=24, min_inclusive=True, max_inclusive=True),
        required=False,
        default=None,
    )
    event_types = EventTypes()
    subscriber_subset = SubscriberSubset()

    __model__ = MostFrequentLocationExposed
