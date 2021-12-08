# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from typing import List

from flowmachine.features.location.labelled_spatial_aggregate import (
    LabelledSpatialAggregate,
)
from flowmachine.core import Query
from flowmachine.core.mixins import GeoDataMixin


class RedactedLabelledSpatialAggregate(GeoDataMixin, Query):
    """
    Query that drops any locations that, when disaggregated by label, reveal a number of subscribers
    less than redaction_threshold
    Parameters
    ----------
    labelled_spatial_aggregate: LabelledSpatialAggregate
        The LabelledSpatialAggregate query to redact
    redaction_threshold: int default 15
        If any labels within a location reveal less than this number of subscribers, that location is dropped
    """

    def __init__(
        self,
        *,
        labelled_spatial_aggregate: LabelledSpatialAggregate,
        redaction_threshold: int = 15,
    ):

        self.labelled_spatial_aggregate = labelled_spatial_aggregate
        self.redaction_threshold = redaction_threshold
        self.spatial_unit = labelled_spatial_aggregate.spatial_unit
        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return self.labelled_spatial_aggregate.column_names

    def _make_query(self):

        aggs = ",".join(
            self.labelled_spatial_aggregate.spatial_unit.location_id_columns
        )
        all_cols = self.labelled_spatial_aggregate.column_names_as_string_list

        sql = f"""
        WITH lsa_query AS(
            SELECT *
            FROM({self.labelled_spatial_aggregate.get_query()}) AS t
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
