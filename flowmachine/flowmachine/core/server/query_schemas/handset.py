# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import fields
from marshmallow.validate import OneOf

from flowmachine.features import SubscriberHandsetCharacteristic
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

__all__ = ["HandsetSchema", "HandsetExposed"]


class HandsetExposed(BaseExposedQueryWithSampling):
    # query_kind class attribute is required for nesting and serialisation
    query_kind = "handset"

    def __init__(
        self,
        *,
        start_date,
        end_date,
        method,
        characteristic,
        event_types,
        subscriber_subset=None,
        sampling=None,
        hours=None
    ):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.start_date = start_date
        self.end_date = end_date
        self.method = method
        self.characteristic = characteristic
        self.event_types = event_types
        self.subscriber_subset = subscriber_subset
        self.sampling = sampling
        self.hours = hours

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
            table=self.event_types,
            subscriber_subset=self.subscriber_subset,
            hours=self.hours,
        )


class HandsetSchema(
    StartAndEndField,
    EventTypesField,
    SubscriberSubsetField,
    HoursField,
    BaseQueryWithSamplingSchema,
):
    __model__ = HandsetExposed

    # query_kind parameter is required here for claims validation
    query_kind = fields.String(validate=OneOf([__model__.query_kind]), required=True)
    characteristic = fields.String(
        validate=OneOf(
            ["hnd_type", "brand", "model", "software_os_name", "software_os_vendor"]
        ),
        required=True,
    )
    method = fields.String(validate=OneOf(["last", "most-common"]), required=True)
