# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import fields
from marshmallow.validate import OneOf

from flowmachine.features import TopUpAmount
from .custom_fields import Statistic
from .base_query_with_sampling import (
    BaseQueryWithSamplingSchema,
    BaseExposedQueryWithSampling,
)
from .field_mixins import HoursField, StartAndEndField, SubscriberSubsetField

__all__ = ["TopUpAmountSchema", "TopUpAmountExposed"]


class TopUpAmountExposed(BaseExposedQueryWithSampling):
    def __init__(
        self,
        *,
        start_date,
        end_date,
        statistic,
        subscriber_subset=None,
        sampling=None,
        hours=None
    ):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.start = start_date
        self.stop = end_date
        self.statistic = statistic
        self.subscriber_subset = subscriber_subset
        self.sampling = sampling
        self.hours = hours

    @property
    def _unsampled_query_obj(self):
        """
        Return the underlying flowmachine topup_amount object.

        Returns
        -------
        Query
        """
        return TopUpAmount(
            start=self.start,
            stop=self.stop,
            statistic=self.statistic,
            subscriber_subset=self.subscriber_subset,
            hours=self.hours,
        )


class TopUpAmountSchema(
    StartAndEndField, SubscriberSubsetField, HoursField, BaseQueryWithSamplingSchema
):
    query_kind = fields.String(validate=OneOf(["topup_amount"]))
    statistic = Statistic()

    __model__ = TopUpAmountExposed
