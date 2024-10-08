# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import fields
from marshmallow.validate import OneOf

from flowmachine.features import SubscriberDegree
from .base_query_with_sampling import (
    BaseQueryWithSamplingSchema,
    BaseExposedQueryWithSampling,
)
from .custom_fields import Direction
from .field_mixins import (
    HoursField,
    StartAndEndField,
    EventTypesField,
    SubscriberSubsetField,
)

__all__ = ["SubscriberDegreeSchema", "SubscriberDegreeExposed"]


class SubscriberDegreeExposed(BaseExposedQueryWithSampling):
    # query_kind class attribute is required for nesting and serialisation
    query_kind = "subscriber_degree"

    def __init__(
        self,
        *,
        start_date,
        end_date,
        direction,
        event_types,
        subscriber_subset=None,
        sampling=None,
        hours=None
    ):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.start = start_date
        self.stop = end_date
        self.direction = direction
        self.event_types = event_types
        self.subscriber_subset = subscriber_subset
        self.sampling = sampling
        self.hours = hours

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
            tables=self.event_types,
            hours=self.hours,
            subscriber_subset=self.subscriber_subset,
        )


class SubscriberDegreeSchema(
    StartAndEndField,
    EventTypesField,
    SubscriberSubsetField,
    HoursField,
    BaseQueryWithSamplingSchema,
):
    __model__ = SubscriberDegreeExposed

    # query_kind parameter is required here for claims validation
    query_kind = fields.String(validate=OneOf([__model__.query_kind]), required=True)
    direction = Direction()
