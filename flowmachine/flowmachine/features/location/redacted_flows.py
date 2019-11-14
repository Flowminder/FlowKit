# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from typing import List

from flowmachine.core import Query
from flowmachine.features import Flows
from flowmachine.features.location.flows import FlowLike


class RedactedFlows(FlowLike, Query):
    """
    An object representing the difference in locations between two location
    type objects, redacted.

    Parameters
    ----------
    flows : Flows
        An unredacted flows object
    """

    def __init__(self, *, flows: Flows):

        self.flows = flows
        # self.spatial_unit is used in self._geo_augmented_query
        self.spatial_unit = flows.spatial_unit
        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return self.flows.column_names

    def _make_query(self):

        sql = f"""
        SELECT
            {self.column_names_as_string_list}
        FROM
            ({self.flows.get_query()}) AS agged
        WHERE agged.value > 15
        """

        return sql

    @property
    def fully_qualified_table_name(self):
        raise NotImplementedError
