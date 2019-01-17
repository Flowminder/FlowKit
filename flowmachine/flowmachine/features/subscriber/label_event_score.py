# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from typing import List, Dict, Union

from .scores import EventScore
from .subscriber_location_cluster import _JoinedHartiganCluster
from ...core import Query


class LabelEventScore(Query):
    """
    Represents a label event score class.

    This class will label a table containing scores based on a labelling
    dictionary. It allows one to specify labels which every subscriber must have.
    This class is used to label locations based on scoring signatures in the
    absence of other automated labelling mechanisms.

    Parameters
    ----------
    scores : flowmachine.Query
        A flowmachine.Query object.
        This represents a table that contains scores which are used to label
        a given location. This table must have a subscriber column (called subscriber).
    labels : dict
        A dictionary whose keys are the label names and the values are
        strings specifying which observations should be labelled with the
        given label. Those rules should be written in the same way as one
        would write a `WHERE` clause in SQL.  Observations which do not
        match any of the criteria are given the reserved label 'unknown'.
        Eg.: \`{'evening': '(score_hour > 0) AND (score_dow > 0.5 OR
        score_dow < -0.5)' , 'daytime': '(score_hour < 0) AND (score_dow <
        0.5 AND score_dow > -0.5)'}\`
    enum_type : str
        The name of the Enumerated type in the database used to represent
        the labels. It is important to ensure that this type does not yet
        exist in the database in case you want to redefine it.
    required :
        Specifies a label which every subscriber must possess independently of
        the score.  This is used in cases where, for instance, we require
        that all subscribers must have an evening/home location.
    """

    def __init__(
        self,
        scores: Union[EventScore, _JoinedHartiganCluster],
        labels={
            "evening": [
                {
                    "hour_lower_bound": 0.00001,
                    "hour_upper_bound": 1,
                    "day_of_week_upper_bound": 1,
                    "day_of_week_lower_bound": 0.5,
                },
                {
                    "hour_lower_bound": 0.00001,
                    "hour_upper_bound": 1,
                    "day_of_week_upper_bound": -0.5,
                    "day_of_week_lower_bound": -1,
                },
            ],
            "day": [
                {
                    "hour_lower_bound": -1,
                    "hour_upper_bound": 0,
                    "day_of_week_upper_bound": 0.5,
                    "day_of_week_lower_bound": -0.5,
                }
            ],
        },
        required: Union[str, None] = None,
    ):

        self.scores = scores
        if not isinstance(scores, Query):
            raise TypeError(
                "Scores must be of type Query, e.g. EventScores, Table, CustomQuery"
            )

        for label, bounds in labels.items():
            for bound_ix, bound in enumerate(bounds):
                try:
                    LabelEventScore.check_bound_is_valid(bound)
                except (ValueError, KeyError) as e:
                    raise ValueError(
                        f"Label {label} bound score bound {bound_ix} invalid: {e}"
                    )
        LabelEventScore.bounds_dict_has_overlaps(labels)
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

    def _make_query(self):

        scores_cols = self.scores.column_names
        scores = f"({self.scores.get_query()}) AS scores"

        sql = f"""
        SELECT *, ({LabelEventScore._get_sql_bound(self.labels)}) AS label FROM {scores}
        """

        if self.required is not None:
            scores_cols = ", ".join([f"labelled.{c}" for c in scores_cols])
            sql = f"""

                WITH labelled AS ({sql}),
                    filtered AS (SELECT l.subscriber AS subscriber FROM labelled l
                                GROUP BY l.subscriber, label HAVING label = '{self.required}')

                SELECT {scores_cols}, '{self.required}' AS label
                FROM labelled
                LEFT JOIN filtered
                ON labelled.subscriber = filtered.subscriber
                WHERE filtered.subscriber IS NULL
                UNION ALL
                SELECT {scores_cols}, label
                FROM labelled
                RIGHT JOIN filtered
                ON labelled.subscriber = filtered.subscriber

            """

        return sql

    @property
    def column_names(self) -> List[str]:
        return self.scores.column_names + ["label"]

    @staticmethod
    def _get_sql_bound(bounds: Dict[str, List[Dict[str, float]]]) -> str:
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
        cases = []
        for label, label_bounds in bounds.items():
            bound_conditions = " OR ".join(
                f"""(score_hour::numeric <@ numrange({b['hour_lower_bound']}, {b['hour_upper_bound']})
                AND 
                score_dow::numeric <@ numrange({b['day_of_week_lower_bound']}, {b['day_of_week_upper_bound']}))"""
                for b in label_bounds
            )
            cases.append(f"WHEN ({bound_conditions}) THEN '{label}'")
        return f"CASE {' '.join(cases)} END"

    @staticmethod
    def check_bound_is_valid(bounds: Dict[str, float]) -> bool:
        """
        Check a boundary is valid (upper threshold greater than lower, all expected keys).
        Raise a ValueError thresholds are wrong, and a KeyError if a key is missing.

        Returns
        -------
        bool
            True if the bound is valid

        """
        for bound_kind in ("day_of_week", "hour"):
            if (
                bounds[f"{bound_kind}_lower_bound"]
                > bounds[f"{bound_kind}_upper_bound"]
            ):
                raise ValueError(
                    f"{bound_kind}_lower_bound is greater than {bound_kind}_upper_bound"
                )
            elif (
                bounds[f"{bound_kind}_lower_bound"]
                == bounds[f"{bound_kind}_upper_bound"]
            ):
                raise ValueError(
                    f"{bound_kind}_lower_bound is the same as {bound_kind}_upper_bound"
                )
        return True

    @staticmethod
    def bounds_dict_has_overlaps(bounds: Dict[str, List[Dict[str, float]]]) -> bool:
        """
        Check if any score boundaries overlap one another, and raise
        an exception identifying the ones that do.

        Parameters
        ----------
        bounds : dict
            Dict mapping labels to lists of score boundaries

        Returns
        -------
        bool
            True if any bounds overlap
        """
        bounds_checked = {}

        # Need to compare all bounds with one another, but would like to
        # only check each _pair_ of bounds once
        flattened_bounds = [
            (k, ix) for k, v in bounds.items() for ix, b in enumerate(v)
        ]
        for label, bound_ix in flattened_bounds:
            cross_checks = [
                (k, ix)
                for k, ix in flattened_bounds
                if k is not label
                and frozenset(((k, ix), (label, bound_ix))) not in bounds_checked
            ]
            overlaps = [
                (k, ix)
                for k, ix in cross_checks
                if LabelEventScore.has_overlap(bounds[label][bound_ix], bounds[k][ix])
            ]
            if len(overlaps) != 0:
                error = f"Labels overlap. Label {label} score bound {bound_ix} overlaps with {' and '.join(f'{k} score bound {ix}' for k, ix in overlaps)}"
                raise ValueError(error)
            bounds_checked.update(
                set(frozenset(((k, ix), (label, bound_ix))) for k, ix in cross_checks)
            )
        return False

    @staticmethod
    def has_overlap(bounds_a: Dict[str, float], bounds_b: Dict[str, float]) -> bool:
        """
        Check that two score boundaries do not overlap.

        Parameters
        ----------
        bounds_a, bounds_b : dict
            Dicts of the form {"hour_lower_bound": -1,
                "hour_upper_bound": 0,
                "day_of_week_upper_bound": 0.5,
                "day_of_week_lower_bound": -0.5}

        Returns
        -------
        bool
            True if the score boundaries overlap one another

        """
        if (bounds_a["hour_lower_bound"] > bounds_b["hour_upper_bound"]) or (
            bounds_b["hour_lower_bound"] > bounds_a["hour_upper_bound"]
        ):
            return False
        if (
            bounds_a["day_of_week_upper_bound"] < bounds_b["day_of_week_lower_bound"]
        ) or (
            bounds_b["day_of_week_upper_bound"] < bounds_a["day_of_week_lower_bound"]
        ):
            return False
        return True
