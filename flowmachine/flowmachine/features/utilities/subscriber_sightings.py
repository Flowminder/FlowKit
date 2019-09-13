# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import structlog
from typing import List

from ...core import Query

logger = structlog.get_logger("flowmachine.debug", submodule=__name__)

class SubscriberSigntings(Query):
    """
    TODO - add something here about how to use the object
    """

    def __init__(
        self,
        columns,
    ):
        self.columns = columns
        
        super().__init__()
    
    @property
    def column_names(self) -> List[str]:
        return []
    
    def _make_query(self):
        return "TODO - add the SQL"
