# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import fields
from marshmallow.validate import OneOf

from flowmachine.features import UniqueLocationCounts
from . import BaseExposedQuery
from .field_mixins import (
    HoursField,
    StartAndEndField,
    EventTypesField,
    SubscriberSubsetField,
)
from .base_schema import BaseSchema
from .aggregation_unit import AggregationUnitMixin

__all__ = ["UniqueLocationCountsSchema", "UniqueLocationCountsExposed"]


class UniqueLocationCountsExposed(BaseExposedQuery):
    # query_kind class attribute is required for nesting and serialisation
    query_kind = "unique_location_counts"

    def __init__(
        self,
        *,
        start_date,
        end_date,
        aggregation_unit,
        event_types,
        subscriber_subset=None,
        hours=None
    ):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.start_date = start_date
        self.end_date = end_date
        self.aggregation_unit = aggregation_unit
        self.event_types = event_types
        self.subscriber_subset = subscriber_subset
        self.hours = hours

    @property
    def _flowmachine_query_obj(self):
        """
        Return the underlying flowmachine unique_location_counts object.

        Returns
        -------
        Query
        """
        return UniqueLocationCounts(
            start=self.start_date,
            stop=self.end_date,
            spatial_unit=self.aggregation_unit,
            tables=self.event_types,
            subscriber_subset=self.subscriber_subset,
            hours=self.hours,
        )


class UniqueLocationCountsSchema(
    StartAndEndField,
    EventTypesField,
    SubscriberSubsetField,
    HoursField,
    AggregationUnitMixin,
    BaseSchema,
):
    __model__ = UniqueLocationCountsExposed

    # query_kind parameter is required here for claims validation
    query_kind = fields.String(validate=OneOf([__model__.query_kind]), required=True)
