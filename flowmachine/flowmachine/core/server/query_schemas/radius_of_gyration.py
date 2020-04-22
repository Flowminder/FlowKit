# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import fields
from marshmallow.validate import OneOf

from flowmachine.features import RadiusOfGyration
from .custom_fields import SubscriberSubset, ISODateTime
from .base_query_with_sampling import (
    BaseQueryWithSamplingSchema,
    BaseExposedQueryWithSampling,
)

__all__ = ["RadiusOfGyrationSchema", "RadiusOfGyrationExposed"]


class RadiusOfGyrationExposed(BaseExposedQueryWithSampling):
    def __init__(self, *, start_date, end_date, subscriber_subset=None, sampling=None):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.start_date = start_date
        self.end_date = end_date
        self.subscriber_subset = subscriber_subset
        self.sampling = sampling

    @property
    def _unsampled_query_obj(self):
        """
        Return the underlying flowmachine radius_of_gyration object.

        Returns
        -------
        Query
        """
        return RadiusOfGyration(
            start=self.start_date,
            stop=self.end_date,
            subscriber_subset=self.subscriber_subset,
        )


class RadiusOfGyrationSchema(BaseQueryWithSamplingSchema):
    # query_kind parameter is required here for claims validation
    query_kind = fields.String(validate=OneOf(["radius_of_gyration"]))
    start_date = ISODateTime(required=True)
    end_date = ISODateTime(required=True)
    subscriber_subset = SubscriberSubset()

    __model__ = RadiusOfGyrationExposed
