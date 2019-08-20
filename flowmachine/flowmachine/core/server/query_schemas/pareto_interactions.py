# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import fields, post_load
from marshmallow.validate import OneOf, Length, Range

from flowmachine.features import ParetoInteractions
from .custom_fields import SubscriberSubset
from .base_query_with_sampling import (
    BaseQueryWithSamplingSchema,
    BaseExposedQueryWithSampling,
)

__all__ = ["ParetoInteractionsSchema", "ParetoInteractionsExposed"]


class ParetoInteractionsSchema(BaseQueryWithSamplingSchema):
    query_kind = fields.String(validate=OneOf(["pareto_interactions"]))
    start = fields.Date(required=True)
    stop = fields.Date(required=True)
    proportion = fields.Float(required=True, validate=Range(min=0.0, max=1.0))
    subscriber_subset = SubscriberSubset()

    @post_load
    def make_query_object(self, params, **kwargs):
        return ParetoInteractionsExposed(**params)


class ParetoInteractionsExposed(BaseExposedQueryWithSampling):
    def __init__(
        self, *, start, stop, proportion, subscriber_subset=None, sampling=None
    ):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.start = start
        self.stop = stop
        self.proportion = proportion
        self.subscriber_subset = subscriber_subset
        self.sampling = sampling

    @property
    def _unsampled_query_obj(self):
        """
        Return the underlying flowmachine pareto_interactions object.

        Returns
        -------
        Query
        """
        return ParetoInteractions(
            start=self.start,
            stop=self.stop,
            proportion=self.proportion,
            subscriber_subset=self.subscriber_subset,
        )
