# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from typing import List, Union

from flowmachine.features.location.labelled_flows import LabelledFlows
from flowmachine.features.location.labelled_spatial_aggregate import (
    LabelledSpatialAggregate,
)
from flowmachine.core import Query


class RedactedLabelledAggregate(Query):
    """
    Query that drops any locations aggregates or flows that, when disaggregated by label, reveal a number of subscribers
    less than redaction_threshold
    Parameters
    ----------
    labelled_query: LabelledSpatialAggregate or LabelledFlows
        The LabelledSpatialAggregate query to redact
    redaction_threshold: int default 15
        If any labels within a location reveal this number of subscribers or fewer, that location is dropped
    """

    def __init__(
        self,
        *,
        labelled_query: Union[LabelledSpatialAggregate, LabelledFlows],
        redaction_threshold: int = 15,
    ):
        if not hasattr(labelled_query, "out_spatial_columns"):
            raise ValueError("labelled_query must implement out_spatial_columns")

        self.labelled_query = labelled_query
        self.redaction_threshold = redaction_threshold
        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return self.labelled_query.column_names

    def _make_query(self):
        aggs = ",".join(self.labelled_query.out_spatial_columns)
        all_cols = self.labelled_query.column_names_as_string_list

        sql = f"""
        WITH lsa_query AS(
            SELECT *
            FROM({self.labelled_query.get_query()}) AS t
        ), aggs_to_keep AS(
            SELECT {aggs}
            FROM lsa_query
            GROUP BY {aggs}
            HAVING min(value) > {self.redaction_threshold}
        )
        SELECT {all_cols}
        FROM lsa_query INNER JOIN aggs_to_keep USING ({aggs})
        """
        return sql
