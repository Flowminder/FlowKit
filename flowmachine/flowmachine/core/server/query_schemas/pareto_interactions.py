# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import fields
from marshmallow.validate import OneOf, Range

from flowmachine.features import ParetoInteractions
from .custom_fields import EventTypes, ISODateTime
from .subscriber_subset import SubscriberSubset
from .base_query_with_sampling import (
    BaseQueryWithSamplingSchema,
    BaseExposedQueryWithSampling,
)

__all__ = ["ParetoInteractionsSchema", "ParetoInteractionsExposed"]


class ParetoInteractionsExposed(BaseExposedQueryWithSampling):
    def __init__(
        self,
        *,
        start,
        stop,
        proportion,
        event_types,
        subscriber_subset=None,
        sampling=None
    ):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.start = start
        self.stop = stop
        self.proportion = proportion
        self.event_types = event_types
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
            tables=self.event_types,
            subscriber_subset=self.subscriber_subset,
        )


class ParetoInteractionsSchema(BaseQueryWithSamplingSchema):
    query_kind = fields.String(validate=OneOf(["pareto_interactions"]))
    start = ISODateTime(required=True)
    stop = ISODateTime(required=True)
    proportion = fields.Float(required=True, validate=Range(min=0.0, max=1.0))
    event_types = EventTypes()
    subscriber_subset = SubscriberSubset()

    __model__ = ParetoInteractionsExposed
