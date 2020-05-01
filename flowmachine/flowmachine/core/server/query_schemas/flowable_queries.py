# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flowmachine.core.server.query_schemas.daily_location import DailyLocationSchema
from flowmachine.core.server.query_schemas.modal_location import ModalLocationSchema
from flowmachine.core.server.query_schemas.unique_locations import UniqueLocationsSchema

flowable_queries = {
    "daily_location": DailyLocationSchema,
    "modal_location": ModalLocationSchema,
    "unique_locations": UniqueLocationsSchema,
}
