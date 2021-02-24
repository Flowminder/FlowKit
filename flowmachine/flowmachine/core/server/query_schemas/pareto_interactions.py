# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import fields
from marshmallow.validate import OneOf, Range

from flowmachine.features import ParetoInteractions
from .base_query_with_sampling import (
    BaseQueryWithSamplingSchema,
    BaseExposedQueryWithSampling,
)
from .field_mixins import (
    HoursField,
    StartAndEndField,
    EventTypesField,
    SubscriberSubsetField,
)

__all__ = ["ParetoInteractionsSchema", "ParetoInteractionsExposed"]


class ParetoInteractionsExposed(BaseExposedQueryWithSampling):
    def __init__(
        self,
        *,
        start_date,
        end_date,
        proportion,
        event_types,
        subscriber_subset=None,
        sampling=None,
        hours=None
    ):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.start = start_date
        self.stop = end_date
        self.proportion = proportion
        self.event_types = event_types
        self.subscriber_subset = subscriber_subset
        self.sampling = sampling
        self.hours = hours

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
            hours=self.hours,
        )


class ParetoInteractionsSchema(
    StartAndEndField,
    EventTypesField,
    SubscriberSubsetField,
    HoursField,
    BaseQueryWithSamplingSchema,
):
    query_kind = fields.String(validate=OneOf(["pareto_interactions"]))
    proportion = fields.Float(required=True, validate=Range(min=0.0, max=1.0))

    __model__ = ParetoInteractionsExposed
