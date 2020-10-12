# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import fields, post_load
from marshmallow.validate import OneOf

from flowmachine.features import UniqueLocationCounts
from . import BaseExposedQuery
from .base_schema import BaseSchema
from .custom_fields import EventTypes, ISODateTime
from .subscriber_subset import SubscriberSubset
from .aggregation_unit import AggregationUnitMixin

__all__ = ["UniqueLocationCountsSchema", "UniqueLocationCountsExposed"]


class UniqueLocationCountsExposed(BaseExposedQuery):
    def __init__(
        self,
        *,
        start_date,
        end_date,
        aggregation_unit,
        event_types,
        subscriber_subset=None,
        sampling=None
    ):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.start_date = start_date
        self.end_date = end_date
        self.aggregation_unit = aggregation_unit
        self.event_types = event_types
        self.subscriber_subset = subscriber_subset

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
        )


class UniqueLocationCountsSchema(AggregationUnitMixin, BaseSchema):
    query_kind = fields.String(validate=OneOf(["unique_location_counts"]))
    start_date = ISODateTime(required=True)
    end_date = ISODateTime(required=True)
    event_types = EventTypes()
    subscriber_subset = SubscriberSubset()

    __model__ = UniqueLocationCountsExposed
