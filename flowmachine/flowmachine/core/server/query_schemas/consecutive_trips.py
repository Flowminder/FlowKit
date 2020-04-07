# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import fields, post_load
from marshmallow.validate import OneOf

from flowmachine.features.location.redacted_consecutive_trips import (
    RedactedConsecutiveTrips,
)
from flowmachine.features.utilities.subscriber_locations import SubscriberLocations
from flowmachine.features.location.consecutive_trips import ConsecutiveTrips
from .custom_fields import SubscriberSubset
from .aggregation_unit import AggregationUnit, get_spatial_unit_obj
from .base_query_with_sampling import (
    BaseQueryWithSamplingSchema,
    BaseExposedQueryWithSampling,
)

__all__ = ["ConsecutiveTripsSchema", "ConsecutiveTripsExposed"]


class ConsecutiveTripsSchema(BaseQueryWithSamplingSchema):
    # query_kind parameter is required here for claims validation
    query_kind = fields.String(validate=OneOf(["consecutive_trips"]))
    start_date = fields.Date(required=True)
    end_date = fields.Date(required=True)
    aggregation_unit = AggregationUnit()
    subscriber_subset = SubscriberSubset()

    @post_load
    def make_query_object(self, params, **kwargs):
        return ConsecutiveTripsExposed(**params)


class ConsecutiveTripsExposed(BaseExposedQueryWithSampling):
    def __init__(
        self,
        start_date,
        end_date,
        *,
        aggregation_unit,
        subscriber_subset=None,
        sampling=None
    ):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.start_date = start_date
        self.end_date = end_date
        self.aggregation_unit = aggregation_unit
        self.subscriber_subset = subscriber_subset
        self.sampling = sampling

    @property
    def _flowmachine_query_obj(self):
        """
        Return the underlying flowmachine object.

        Returns
        -------
        Query
        """
        return RedactedConsecutiveTrips(
            consecutive_trips=ConsecutiveTrips(
                SubscriberLocations(
                    self.start_date,
                    self.end_date,
                    spatial_unit=get_spatial_unit_obj(self.aggregation_unit),
                    subscriber_subset=self.subscriber_subset,
                )
            )
        )
