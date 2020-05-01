# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flowmachine.core.server.query_schemas.displacement import DisplacementSchema
from flowmachine.core.server.query_schemas.event_count import EventCountSchema
from flowmachine.core.server.query_schemas.nocturnal_events import NocturnalEventsSchema
from flowmachine.core.server.query_schemas.pareto_interactions import (
    ParetoInteractionsSchema,
)
from flowmachine.core.server.query_schemas.radius_of_gyration import (
    RadiusOfGyrationSchema,
)
from flowmachine.core.server.query_schemas.subscriber_degree import (
    SubscriberDegreeSchema,
)
from flowmachine.core.server.query_schemas.topup_amount import TopUpAmountSchema
from flowmachine.core.server.query_schemas.topup_balance import TopUpBalanceSchema
from flowmachine.core.server.query_schemas.unique_location_counts import (
    UniqueLocationCountsSchema,
)

histogrammable_queries = {
    "radius_of_gyration": RadiusOfGyrationSchema,
    "unique_location_counts": UniqueLocationCountsSchema,
    "topup_balance": TopUpBalanceSchema,
    "subscriber_degree": SubscriberDegreeSchema,
    "topup_amount": TopUpAmountSchema,
    "event_count": EventCountSchema,
    "pareto_interactions": ParetoInteractionsSchema,
    "nocturnal_events": NocturnalEventsSchema,
    "displacement": DisplacementSchema,
}
