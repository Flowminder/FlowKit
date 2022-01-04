# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow_oneofschema import OneOfSchema

from flowmachine.core.server.query_schemas.coalesced_location import (
    CoalescedLocationSchema,
)
from flowmachine.core.server.query_schemas.daily_location import DailyLocationSchema
from flowmachine.core.server.query_schemas.majority_location import (
    MajorityLocationSchema,
)
from flowmachine.core.server.query_schemas.modal_location import ModalLocationSchema
from flowmachine.core.server.query_schemas.most_frequent_location import (
    MostFrequentLocationSchema,
)
from flowmachine.core.server.query_schemas.visited_most_days import (
    VisitedMostDaysSchema,
)


class ReferenceLocationSchema(OneOfSchema):
    """
    A set of queries that return a mapping between unique subscribers and locations
    """

    type_field = "query_kind"
    type_schemas = {
        "daily_location": DailyLocationSchema,
        "modal_location": ModalLocationSchema,
        "most_frequent_location": MostFrequentLocationSchema,
        "visited_most_days": VisitedMostDaysSchema,
        "majority_location": MajorityLocationSchema,
        "coalesced_location": CoalescedLocationSchema,
    }
