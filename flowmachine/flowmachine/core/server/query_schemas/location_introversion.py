# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import fields
from marshmallow.validate import OneOf

from flowmachine.features import LocationIntroversion
from flowmachine.features.location.redacted_location_introversion import (
    RedactedLocationIntroversion,
)
from .custom_fields import EventTypes
from .base_exposed_query import BaseExposedQuery
from .aggregation_unit import AggregationUnitMixin

__all__ = ["LocationIntroversionSchema", "LocationIntroversionExposed"]

from .base_schema import BaseSchema
from .custom_fields import ISODateTime
from .subscriber_subset import SubscriberSubset


class LocationIntroversionExposed(BaseExposedQuery):
    def __init__(
        self,
        *,
        start_date,
        end_date,
        aggregation_unit,
        direction,
        event_types,
        subscriber_subset=None
    ):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.start_date = start_date
        self.end_date = end_date
        self.aggregation_unit = aggregation_unit
        self.direction = direction
        self.event_types = event_types
        self.subscriber_subset = subscriber_subset

    @property
    def _flowmachine_query_obj(self):
        """
        Return the underlying flowmachine location_introversion object.

        Returns
        -------
        Query
        """
        return RedactedLocationIntroversion(
            location_introversion=LocationIntroversion(
                start=self.start_date,
                stop=self.end_date,
                spatial_unit=self.aggregation_unit,
                direction=self.direction,
                table=self.event_types,
                subscriber_subset=self.subscriber_subset,
            )
        )


class LocationIntroversionSchema(AggregationUnitMixin, BaseSchema):
    # query_kind parameter is required here for claims validation
    query_kind = fields.String(validate=OneOf(["location_introversion"]))
    start_date = ISODateTime(required=True)
    end_date = ISODateTime(required=True)
    direction = fields.String(
        required=False, validate=OneOf(["in", "out", "both"]), default="both"
    )  # TODO: use a globally defined enum for this
    event_types = EventTypes()
    subscriber_subset = SubscriberSubset()

    __model__ = LocationIntroversionExposed
