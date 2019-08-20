# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import fields, post_load
from marshmallow.validate import OneOf

from flowmachine.features import daily_location
from .custom_fields import SubscriberSubset
from .aggregation_unit import AggregationUnit, get_spatial_unit_obj
from .base_query_with_sampling import (
    BaseQueryWithSamplingSchema,
    BaseExposedQueryWithSampling,
)

__all__ = ["DailyLocationSchema", "DailyLocationExposed"]


class DailyLocationSchema(BaseQueryWithSamplingSchema):
    # query_kind parameter is required here for claims validation
    query_kind = fields.String(validate=OneOf(["daily_location"]))
    date = fields.Date(required=True)
    method = fields.String(required=True, validate=OneOf(["last", "most-common"]))
    aggregation_unit = AggregationUnit()
    subscriber_subset = SubscriberSubset()

    @post_load
    def make_query_object(self, params, **kwargs):
        return DailyLocationExposed(**params)


class DailyLocationExposed(BaseExposedQueryWithSampling):
    def __init__(
        self, date, *, method, aggregation_unit, subscriber_subset=None, sampling=None
    ):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.date = date
        self.method = method
        self.aggregation_unit = aggregation_unit
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
        return daily_location(
            date=self.date,
            spatial_unit=get_spatial_unit_obj(self.aggregation_unit),
            method=self.method,
            subscriber_subset=self.subscriber_subset,
        )
