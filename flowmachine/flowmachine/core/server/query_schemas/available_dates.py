# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import Schema, post_load

from flowmachine.core.available_dates import AvailableDates
from .base_exposed_query import BaseExposedQuery
from .custom_fields import EventTypes

__all__ = ["AvailableDatesSchema", "AvailableDatesExposed"]


class AvailableDatesSchema(Schema):
    event_types = EventTypes()

    @post_load
    def make_query_object(self, params):
        return AvailableDatesExposed(**params)


class AvailableDatesExposed(BaseExposedQuery):
    def __init__(self, *, event_types):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.event_types = event_types

    @property
    def _flowmachine_query_obj(self):
        """
        Return the underlying flowmachine query object.

        Returns
        -------
        Query
        """
        return AvailableDates(event_types=self.event_types)
