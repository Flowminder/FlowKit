# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import fields

from flowmachine.core.server.query_schemas.custom_fields import (
    Hours,
    ISODateTime,
    EventTypes,
)
from flowmachine.core.server.query_schemas.subscriber_subset import SubscriberSubset


class HoursField:
    hours = fields.Nested(Hours, missing=None, allow_none=True)


class StartAndEndField:
    start_date = ISODateTime(required=True)
    end_date = ISODateTime(required=True)


class EventTypesField:
    event_types = EventTypes()


class SubscriberSubsetField:
    subscriber_subset = SubscriberSubset()
