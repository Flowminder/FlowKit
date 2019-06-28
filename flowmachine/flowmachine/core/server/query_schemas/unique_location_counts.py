# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import Schema, fields, post_load
from marshmallow.validate import OneOf, Length

from flowmachine.features import UniqueLocationCounts
from .base_exposed_query import BaseExposedQuery
from .custom_fields import SubscriberSubset
from .aggregation_unit import AggregationUnit, get_spatial_unit_obj

__all__ = ["UniqueLocationCountsSchema", "UniqueLocationCountsExposed"]


class UniqueLocationCountsSchema(Schema):
    query_kind = fields.String(validate=OneOf(["unique_location_counts"]))
    start_date = fields.Date(required=True)
    end_date = fields.Date(required=True)
    aggregation_unit = AggregationUnit()
    subscriber_subset = SubscriberSubset()

    @post_load
    def make_query_object(self, params, **kwargs):
        return UniqueLocationCountsExposed(**params)


class UniqueLocationCountsExposed(BaseExposedQuery):
    def __init__(
        self, *, start_date, end_date, aggregation_unit, subscriber_subset=None
    ):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.start_date = start_date
        self.end_date = end_date
        self.aggregation_unit = aggregation_unit
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
            spatial_unit=get_spatial_unit_obj(self.aggregation_unit),
            subscriber_subset=self.subscriber_subset,
        )
