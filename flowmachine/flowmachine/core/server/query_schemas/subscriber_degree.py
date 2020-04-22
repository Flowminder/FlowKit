# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import fields
from marshmallow.validate import OneOf

from flowmachine.features import SubscriberDegree
from .custom_fields import SubscriberSubset, ISODateTime
from .base_query_with_sampling import (
    BaseQueryWithSamplingSchema,
    BaseExposedQueryWithSampling,
)

__all__ = ["SubscriberDegreeSchema", "SubscriberDegreeExposed"]


class SubscriberDegreeExposed(BaseExposedQueryWithSampling):
    def __init__(
        self, *, start, stop, direction, subscriber_subset=None, sampling=None
    ):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.start = start
        self.stop = stop
        self.direction = direction
        self.subscriber_subset = subscriber_subset
        self.sampling = sampling

    @property
    def _unsampled_query_obj(self):
        """
        Return the underlying flowmachine subscriber_degree object.

        Returns
        -------
        Query
        """
        return SubscriberDegree(
            start=self.start,
            stop=self.stop,
            direction=self.direction,
            subscriber_subset=self.subscriber_subset,
        )


class SubscriberDegreeSchema(BaseQueryWithSamplingSchema):
    query_kind = fields.String(validate=OneOf(["subscriber_degree"]))
    start = ISODateTime(required=True)
    stop = ISODateTime(required=True)
    direction = fields.String(
        required=False, validate=OneOf(["in", "out", "both"]), default="both"
    )  # TODO: use a globally defined enum for this
    subscriber_subset = SubscriberSubset()

    __model__ = SubscriberDegreeExposed
