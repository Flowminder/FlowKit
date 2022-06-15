# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

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
from flowmachine.core.server.query_schemas.one_of_query import OneOfQuerySchema


class ReferenceLocationSchema(OneOfQuerySchema):
    """
    A set of queries that return a mapping between unique subscribers and locations
    """

    query_schemas = (
        DailyLocationSchema,
        ModalLocationSchema,
        MostFrequentLocationSchema,
        VisitedMostDaysSchema,
        MajorityLocationSchema,
        CoalescedLocationSchema,
    )
