# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import fields
from marshmallow.validate import OneOf

from flowmachine.features import Displacement
from .custom_fields import SubscriberSubset, Statistic, ISODateTime

from .base_query_with_sampling import (
    BaseQueryWithSamplingSchema,
    BaseExposedQueryWithSampling,
)
from .metric_types import ContinuousMetric
from .reference_location import ReferenceLocationSchema

__all__ = ["DisplacementSchema", "DisplacementExposed"]


class DisplacementExposed(BaseExposedQueryWithSampling):
    def __init__(
        self,
        *,
        start,
        stop,
        statistic,
        reference_location,
        subscriber_subset=None,
        sampling=None
    ):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.start = start
        self.stop = stop
        self.statistic = statistic
        self.reference_location = reference_location
        self.subscriber_subset = subscriber_subset
        self.sampling = sampling

    @property
    def _unsampled_query_obj(self):
        """
        Return the underlying flowmachine displacement object.

        Returns
        -------
        Query
        """
        return Displacement(
            start=self.start,
            stop=self.stop,
            statistic=self.statistic,
            reference_location=self.reference_location._flowmachine_query_obj,
            subscriber_subset=self.subscriber_subset,
        )


class DisplacementSchema(ContinuousMetric, BaseQueryWithSamplingSchema):
    query_kind = fields.String(validate=OneOf(["displacement"]))
    start = ISODateTime(required=True)
    stop = ISODateTime(required=True)
    statistic = Statistic()
    reference_location = fields.Nested(ReferenceLocationSchema, many=False)
    subscriber_subset = SubscriberSubset()

    __model__ = DisplacementExposed
