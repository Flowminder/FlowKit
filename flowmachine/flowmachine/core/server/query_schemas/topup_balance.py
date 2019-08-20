# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import fields, post_load
from marshmallow.validate import OneOf, Length

from flowmachine.features import TopUpBalance
from .custom_fields import Statistic, SubscriberSubset
from .base_query_with_sampling import (
    BaseQueryWithSamplingSchema,
    BaseExposedQueryWithSampling,
)

__all__ = ["TopUpBalanceSchema", "TopUpBalanceExposed"]


class TopUpBalanceSchema(BaseQueryWithSamplingSchema):
    query_kind = fields.String(validate=OneOf(["topup_balance"]))
    start_date = fields.Date(required=True)
    end_date = fields.Date(required=True)
    statistic = Statistic()
    subscriber_subset = SubscriberSubset()

    @post_load
    def make_query_object(self, params, **kwargs):
        return TopUpBalanceExposed(**params)


class TopUpBalanceExposed(BaseExposedQueryWithSampling):
    def __init__(
        self,
        *,
        start_date,
        end_date,
        statistic="avg",
        subscriber_subset=None,
        sampling=None
    ):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.start_date = start_date
        self.end_date = end_date
        self.statistic = statistic
        self.subscriber_subset = subscriber_subset
        self.sampling = sampling

    @property
    def _unsampled_query_obj(self):
        """
        Return the underlying flowmachine TopUpBalance object.

        Returns
        -------
        Query
        """
        return TopUpBalance(
            start=self.start_date,
            stop=self.end_date,
            statistic=self.statistic,
            subscriber_subset=self.subscriber_subset,
        )
