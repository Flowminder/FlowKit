# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import fields, post_load
from marshmallow.validate import OneOf

from flowmachine.features import SubscriberHandsetCharacteristic
from .custom_fields import SubscriberSubset, ISODateTime
from .base_query_with_sampling import (
    BaseQueryWithSamplingSchema,
    BaseExposedQueryWithSampling,
)

__all__ = ["HandsetSchema", "HandsetExposed"]


class HandsetExposed(BaseExposedQueryWithSampling):
    def __init__(
        self,
        *,
        start_date,
        end_date,
        method,
        characteristic,
        subscriber_subset=None,
        sampling=None
    ):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.start_date = start_date
        self.end_date = end_date
        self.method = method
        self.characteristic = characteristic
        self.subscriber_subset = subscriber_subset
        self.sampling = sampling

    @property
    def _unsampled_query_obj(self):
        """
        Return the underlying flowmachine handset object.

        Returns
        -------
        Query
        """
        return SubscriberHandsetCharacteristic(
            start=self.start_date,
            stop=self.end_date,
            characteristic=self.characteristic,
            method=self.method,
            subscriber_subset=self.subscriber_subset,
        )


class HandsetSchema(BaseQueryWithSamplingSchema):
    # query_kind parameter is required here for claims validation
    query_kind = fields.String(validate=OneOf(["handset"]))
    start_date = ISODateTime(required=True)
    end_date = ISODateTime(required=True)
    characteristic = fields.String(
        validate=OneOf(
            ["hnd_type", "brand", "model", "software_os_name", "software_os_vendor"]
        )
    )
    method = fields.String(validate=OneOf(["last", "most-common"]))
    subscriber_subset = SubscriberSubset()

    __model__ = HandsetExposed
