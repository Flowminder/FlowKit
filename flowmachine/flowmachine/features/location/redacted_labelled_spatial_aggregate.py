from typing import List

from flowmachine.features.location.labelled_spatial_aggregate import (
    LabelledSpatialAggregate,
)
from flowmachine.core import Query
from flowmachine.core.mixins import GeoDataMixin


class RedactedLabelledSpatialAggregate(GeoDataMixin, Query):
    """
    Query that drops and reaggregates data if any 'label' column falls below redcation_threshold
    Parameters
    ----------
    labelled_spatial_aggregate
    redaction_threshold
    """

    def __init__(
        self,
        *,
        labelled_spatial_aggregate: LabelledSpatialAggregate,
        redaction_threshold=15,
    ):

        self.labelled_spatial_aggregate = labelled_spatial_aggregate
        self.redaction_threshold = redaction_threshold
        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return self.labelled_spatial_aggregate.column_names

    def _make_query(self):

        aggs = ",".join(
            self.labelled_spatial_aggregate.spatial_unit.location_id_columns
        )
        labels = self.labelled_spatial_aggregate.out_label_columns_as_string_list
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
