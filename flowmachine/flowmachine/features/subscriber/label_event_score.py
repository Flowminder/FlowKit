# This Source Code Form is subject to the terms of the Mozilla Public
# # License, v. 2.0. If a copy of the MPL was not distributed with this
# # file, You can obtain one at http://mozilla.org/MPL/2.0/.
from shapely.geometry import shape
from shapely.geometry.base import BaseGeometry
from typing import List, Dict, Union, Any

from .scores import EventScore
from .hartigan_cluster import _JoinedHartiganCluster
from ...core import Query


class LabelEventScore(Query):
    """
    Represents a label event score class.

    This class will label a table containing scores based on a labelling
    dictionary. It allows one to specify labels which every subscriber must have.
    This class is used to label locations based on scoring signatures in the
    absence of other automated labelling mechanisms.

    Returns the original query object, with an added `label` column.

    Parameters
    ----------
    scores : flowmachine.Query
        A flowmachine.Query object.
        This represents a table that contains scores which are used to label
        a given location. This table must have a subscriber column (called subscriber).
    labels : dict
        A dictionary whose keys are the label names and the values geojson shapes,
        specified hour of day, and day of week score, with hour of day score on the x axis
        and day of week score on the y axis, where all scores are real numbers in the range [-1.0, +1.0]
    required : str or None, default None
        Optionally specifies a label which every subscriber must possess independently of
        the score.  This is used in cases where, for instance, we require
        that all subscribers must have an evening/home location.

    Examples
    --------
    >>> es = EventScore(start="2016-01-01", stop="2016-01-05", spatial_unit=make_spatial_unit("versioned-site"))
    >>> es.head()
             subscriber site_id  version        lon        lat  score_hour  score_dow
    0  ZYPxqVGLzlQy6l7n  QeBRM8        0  82.914285  29.358975         1.0       -1.0
    1  4oLKbnxm3vXqjMVx  zdNQx2        0  87.265225  27.585096        -1.0        1.0
    2  vKVLDx8koQWZ2ez0  LVnDQL        0  86.551302  27.245265         0.0       -1.0
    3  DELmRj9Vvl346G50  m9jL23        0  82.601710  29.815919         1.0       -1.0
    4  lqOknAJRDNAewM10  RZgwVz        0  84.623447  28.283523        -1.0        1.0
    >>> ls = LabelEventScore(
            scores=es,
            labels={
                "daytime": {
                    "type": "Polygon",
                    "coordinates": [[[-1.1, -1.1], [-1, 1.1], [1.1, 1.1], [1.1, -1.1]]],
                }
            },
        )
    >>> ls.head()
         label        subscriber site_id  version        lon        lat  score_hour  score_dow
    0  daytime  ZYPxqVGLzlQy6l7n  QeBRM8        0  82.914285  29.358975         1.0       -1.0
    1  daytime  4oLKbnxm3vXqjMVx  zdNQx2        0  87.265225  27.585096        -1.0        1.0
    2  daytime  vKVLDx8koQWZ2ez0  LVnDQL        0  86.551302  27.245265         0.0       -1.0
    3  daytime  DELmRj9Vvl346G50  m9jL23        0  82.601710  29.815919         1.0       -1.0
    4  daytime  lqOknAJRDNAewM10  RZgwVz        0  84.623447  28.283523        -1.0        1.0
    """

    def __init__(
        self,
        *,
        scores: Union[EventScore, _JoinedHartiganCluster],
        labels: Dict[str, Dict[str, Any]] = {
            "evening": {
                "type": "MultiPolygon",
                "coordinates": [
                    [[[0.000_001, 0.5], [0.000_001, 1], [1, 1], [1, 0.5]]],
                    [[[0.000_001, -1], [0.000_001, -0.5], [1, -0.5], [1, -1]]],
                ],
            },
            "day": {
                "type": "Polygon",
                "coordinates": [[[-1, -0.5], [-1, 0.5], [0, 0.5], [0, -0.5]]],
            },
        },
        required: Union[str, None] = None,
    ):

        self.scores = scores
        if not isinstance(scores, Query):
            raise TypeError(
                "Scores must be of type Query, e.g. EventScores, Table, CustomQuery"
            )

        labels = LabelEventScore._convert_bounds_to_shapely_polygons(labels)
        LabelEventScore.verify_bounds_dict_has_no_overlaps(labels)
        self.labels = labels

        self.label_names = list(labels.keys())
        self.required = required
        if "unknown" in self.label_names:
            raise ValueError(
                "'unknown' is a reserved label name, please use another name"
            )
        else:
            self.label_names = ["unknown"] + self.label_names

        super().__init__()

    @staticmethod
    def _convert_bounds_to_shapely_polygons(
        geojson_labels: Dict[str, Dict[str, Any]]
    ) -> Dict[str, BaseGeometry]:
        """
        Takes a dictionary of labels and bounds expressed as lists of geojson shapes
        and returns a dictionary of labels and bounds expressed as Shapely polygons.

        Parameters
        ----------
        geojson_labels : dict
            String -> geojson mappings
        Returns
        -------
        dict
            Dict of labels mapped to lists of shapely polygons

        """
        bounds_dict = {}
        for label, geom in geojson_labels.items():
            try:
                bounds_dict[label] = shape(geom)
            except (AttributeError, IndexError, ValueError) as e:
                raise ValueError(f"Geometry for {label} is not valid: {e}")
        return bounds_dict

    def _make_query(self):

        scores_cols = self.scores.column_names
        scores = f"({self.scores.get_query()}) AS scores"

        sql = f"""
        SELECT COALESCE(score_bounds.label, 'unknown') as label, scores.* FROM {scores}
        LEFT JOIN {LabelEventScore._get_bound_as_sql(self.labels)}
        ON ST_Contains(score_bounds.geom, ST_Point(scores.score_hour, scores.score_dow))
        """

        if self.required is not None:
            scores_cols = ", ".join([f"labelled.{c}" for c in scores_cols])
            sql = f"""

                WITH labelled AS ({sql}),
                    filtered AS (SELECT subscriber AS subscriber FROM labelled
                                GROUP BY subscriber, label HAVING label = '{self.required}')

                SELECT '{self.required}' AS label, {scores_cols}
                FROM labelled
                LEFT JOIN filtered
                ON labelled.subscriber = filtered.subscriber
                WHERE filtered.subscriber IS NULL
                UNION ALL
                SELECT label, {scores_cols}
                FROM labelled
                RIGHT JOIN filtered
                ON labelled.subscriber = filtered.subscriber

            """

        return sql

    @property
    def column_names(self) -> List[str]:
        return ["label"] + self.scores.column_names

    @staticmethod
    def _get_bound_as_sql(bounds: Dict[str, List[Dict[str, float]]]) -> str:
        """
        Translate a dict of label and lists of score boundary dictionaries into an sql case
        statement.

        Parameters
        ----------
        bounds : dict
            Dictionary of labels, with lists of score boundaries that should have that label


        Returns
        -------
        str
            SQL case statement

        """
        table_rows = []
        for label, label_bounds in bounds.items():
            table_rows.append(f"('{label}', '{label_bounds.wkt}'::geometry)")
        return f"(VALUES {', '.join(table_rows)}) as score_bounds(label, geom)"

    @staticmethod
    def verify_bounds_dict_has_no_overlaps(bounds: Dict[str, BaseGeometry]) -> bool:
        """
        Check if any score boundaries overlap one another, and raise
        an exception identifying the ones that do.

        Parameters
        ----------
        bounds : dict
            Dict mapping labels to lists of score boundaries expressed as shapely polygons

        Returns
        -------
        bool
            True if none of the bounds overlap, otherwise False.
        """

        flattened_bounds = [
            (ix, *label_and_bound) for ix, label_and_bound in enumerate(bounds.items())
        ]

        for ix, label, bound in flattened_bounds:
            for ix_b, label_b, bound_b in flattened_bounds[ix + 1 :]:
                if bound.intersects(bound_b):
                    error = f"Label '{label}' bounds overlaps with that of '{label_b}'."
                    raise ValueError(error)
        return True
