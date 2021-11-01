# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, you can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import fields
from marshmallow.validate import OneOf

from .base_query_with_sampling import (
    BaseQueryWithSamplingSchema,
    BaseExposedQueryWithSampling,
)
from flowmachine.features.utilities.subscriber_locations import SubscriberLocations
from flowmachine.features.subscriber.visited_most_days import VisitedMostDays


__all__ = [
    "VisitedMostDaysSchema",
    "VisitedMostDaysExposed",
]


class VisitedMostDaysExposed(BaseExposedQueryWithSampling):
    def __init__(self, *, subscriber_locations: SubscriberLocations = None):
        self.subscriber_locations = subscriber_locations

    @property
    def _unsampled_query_obj(self):
        """
        Return the underlying flowmachine daily_location object.

        Returns
        -------
        Query
        """
        return VisitedMostDays(
            subscriber_locations=self.subscriber_locations,
        )


class VisitedMostDaysSchema(
    BaseQueryWithSamplingSchema,
):
    query_kind = fields.String(validate=OneOf(["visited_most_days"]))

    __model__ = VisitedMostDaysExposed
