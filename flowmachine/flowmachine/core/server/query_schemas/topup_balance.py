# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import fields
from marshmallow.validate import OneOf

from flowmachine.features import TopUpBalance
from .custom_fields import Statistic
from .base_query_with_sampling import (
    BaseQueryWithSamplingSchema,
    BaseExposedQueryWithSampling,
)
from .field_mixins import (
    HoursField,
    StartAndEndField,
    SubscriberSubsetField,
)

__all__ = ["TopUpBalanceSchema", "TopUpBalanceExposed"]


class TopUpBalanceExposed(BaseExposedQueryWithSampling):
    # query_kind class attribute is required for nesting and serialisation
    query_kind = "topup_balance"

    def __init__(
        self,
        *,
        start_date,
        end_date,
        statistic="avg",
        subscriber_subset=None,
        sampling=None,
        hours=None
    ):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.start_date = start_date
        self.end_date = end_date
        self.statistic = statistic
        self.subscriber_subset = subscriber_subset
        self.sampling = sampling
        self.hours = hours

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
            hours=self.hours,
        )


class TopUpBalanceSchema(
    StartAndEndField,
    SubscriberSubsetField,
    HoursField,
    BaseQueryWithSamplingSchema,
):
    __model__ = TopUpBalanceExposed

    # query_kind parameter is required here for claims validation
    query_kind = fields.String(validate=OneOf([__model__.query_kind]), required=True)
    statistic = Statistic()
