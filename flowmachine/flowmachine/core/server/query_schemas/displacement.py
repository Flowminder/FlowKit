# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import fields
from marshmallow.validate import OneOf
from marshmallow_oneofschema import OneOfSchema

from flowmachine.features import Displacement
from .custom_fields import EventTypes, Statistic, ISODateTime, Hours
from .reference_location import ReferenceLocationSchema
from .subscriber_subset import SubscriberSubset
from .daily_location import DailyLocationSchema
from .modal_location import ModalLocationSchema
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

__all__ = ["DisplacementSchema", "DisplacementExposed"]


class DisplacementExposed(BaseExposedQueryWithSampling):
    def __init__(
        self,
        *,
        start_date,
        end_date,
        statistic,
        reference_location,
        event_types,
        subscriber_subset=None,
        sampling=None,
        hours=None
    ):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.start = start_date
        self.stop = end_date
        self.statistic = statistic
        self.reference_location = reference_location
        self.event_types = event_types
        self.subscriber_subset = subscriber_subset
        self.sampling = sampling
        self.hours = hours

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
            table=self.event_types,
            subscriber_subset=self.subscriber_subset,
            hours=self.hours,
        )


class DisplacementSchema(
    StartAndEndField,
    EventTypesField,
    SubscriberSubsetField,
    HoursField,
    BaseQueryWithSamplingSchema,
):
    query_kind = fields.String(validate=OneOf(["displacement"]))
    statistic = Statistic()
    reference_location = fields.Nested(ReferenceLocationSchema, many=False)

    __model__ = DisplacementExposed
