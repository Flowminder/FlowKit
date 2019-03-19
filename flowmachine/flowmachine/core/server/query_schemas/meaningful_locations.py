from marshmallow import Schema, fields, post_load
from marshmallow.validate import OneOf, Range, Length
from typing import Union, Dict, List

from flowmachine.features import (
    MeaningfulLocations,
    MeaningfulLocationsOD,
    MeaningfulLocationsAggregate,
    HartiganCluster,
    CallDays,
    EventScore,
    subscriber_locations,
)
from .base_exposed_query import BaseExposedQuery
from .custom_fields import TowerHourOfDayScores, TowerDayOfWeekScores

__all__ = [
    "MeaningfulLocationsAggregateSchema",
    "MeaningfulLocationsAggregateExposed",
    "MeaningfulLocationsBetweenLabelODMatrixSchema",
    "MeaningfulLocationsBetweenLabelODMatrixExposed",
]


class MeaningfulLocationsAggregateSchema(Schema):
    start_date = fields.Date(required=True)
    stop_date = fields.Date(required=True)
    label = fields.String(required=True)
    labels = fields.Dict(
        required=True, keys=fields.String(), values=fields.Dict()
    )  # TODO: use custom field here for stricter validation!
    tower_hour_of_day_scores = TowerHourOfDayScores(required=True)
    tower_day_of_week_scores = TowerDayOfWeekScores(required=True)
    aggregation_unit = fields.String(
        required=True, validate=OneOf(["admin0", "admin1", "admin2", "admin3"])
    )
    tower_cluster_radius = fields.Float(required=False, default=1.0)
    tower_cluster_call_threshold: fields.Integer(required=False, default=0)
    subscriber_subset: fields.String(
        required=False, allow_none=True, validate=OneOf([None])
    )

    @post_load
    def make_query_object(self, params):
        return MeaningfulLocationsAggregateExposed(**params)


def _make_meaningful_locations_object(
    *,
    start_date,
    stop_date,
    label,
    labels,
    subscriber_subset,
    tower_cluster_call_threshold,
    tower_cluster_radius,
    tower_day_of_week_scores,
    tower_hour_of_day_scores,
):
    q_subscriber_locations = subscriber_locations(
        start=start_date,
        stop=stop_date,
        level="versioned-site",  # note this 'level' is not the same as the exposed parameter 'aggregation_unit'
        subscriber_subset=subscriber_subset,
    )
    q_call_days = CallDays(subscriber_locations=q_subscriber_locations)
    q_hartigan_cluster = HartiganCluster(
        calldays=q_call_days,
        radius=tower_cluster_radius,
        call_threshold=tower_cluster_call_threshold,
        buffer=0,  # we're not exposing 'buffer', apparently, so we're hard-coding it
    )
    q_event_score = EventScore(
        start=start_date,
        stop=stop_date,
        score_hour=tower_hour_of_day_scores,
        score_dow=tower_day_of_week_scores,
        level="versioned-site",  # note this 'level' is not the same as the exposed parameter 'aggregation_unit'
        subscriber_subset=subscriber_subset,
    )
    q_meaningful_locations = MeaningfulLocations(
        clusters=q_hartigan_cluster, labels=labels, scores=q_event_score, label=label
    )
    return q_meaningful_locations


class MeaningfulLocationsAggregateExposed(BaseExposedQuery):

    __schema__ = MeaningfulLocationsAggregateSchema

    def __init__(
        self,
        *,
        start_date: str,
        stop_date: str,
        aggregation_unit: str,
        label: str,
        labels: Dict[str, Dict[str, dict]],
        tower_day_of_week_scores: Dict[str, float],
        tower_hour_of_day_scores: List[float],
        tower_cluster_radius: float = 1.0,
        tower_cluster_call_threshold: int = 0,
        subscriber_subset: Union[dict, None] = None,
    ):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.start_date = start_date
        self.stop_date = stop_date
        self.aggregation_unit = aggregation_unit
        self.label = label
        self.labels = labels
        self.tower_day_of_week_scores = tower_day_of_week_scores
        self.tower_hour_of_day_scores = tower_hour_of_day_scores
        self.tower_cluster_radius = tower_cluster_radius
        self.tower_cluster_call_threshold = tower_cluster_call_threshold
        self.subscriber_subset = subscriber_subset

        q_meaningful_locations = _make_meaningful_locations_object(
            label=label,
            labels=labels,
            start_date=start_date,
            stop_date=stop_date,
            subscriber_subset=subscriber_subset,
            tower_cluster_call_threshold=tower_cluster_call_threshold,
            tower_cluster_radius=tower_cluster_radius,
            tower_day_of_week_scores=tower_day_of_week_scores,
            tower_hour_of_day_scores=tower_hour_of_day_scores,
        )
        self.q_meaningful_locations_aggreate = MeaningfulLocationsAggregate(
            meaningful_locations=q_meaningful_locations, level=aggregation_unit
        )

        super().__init__()  # NOTE: this *must* be called at the end of the __init__() method of any subclass of BaseExposedQuery

    @property
    def _flowmachine_query_obj(self):
        """
        Return the underlying flowmachine MeaningfulLocationsAggregate object.

        Returns
        -------
        ModalLocation
        """
        return self.q_meaningful_locations_aggreate


class MeaningfulLocationsBetweenLabelODMatrixSchema(Schema):
    start_date = fields.Date(required=True)
    stop_date = fields.Date(required=True)
    label_a = fields.String(required=True)
    label_b = fields.String(required=True)
    labels = fields.Dict(
        keys=fields.String(), values=fields.Dict()
    )  # TODO: use custom field here for stricter validation!
    tower_hour_of_day_scores = TowerHourOfDayScores(required=True)
    tower_day_of_week_scores = TowerDayOfWeekScores(required=True)
    tower_cluster_radius = fields.Float(required=False, default=1.0)
    tower_cluster_call_threshold: fields.Integer(required=False, default=0)
    subscriber_subset: fields.String(
        required=False, allow_none=True, validate=OneOf([None])
    )

    @post_load
    def make_query_object(self, params):
        return MeaningfulLocationsBetweenLabelODMatrixExposed(**params)


class MeaningfulLocationsBetweenLabelODMatrixExposed(BaseExposedQuery):

    __schema__ = MeaningfulLocationsBetweenLabelODMatrixSchema

    def __init__(
        self,
        *,
        start_date: str,
        stop_date: str,
        aggregation_unit: str,
        label_a: str,
        label_b: str,
        labels: Dict[str, Dict[str, dict]],
        tower_day_of_week_scores: Dict[str, float],
        tower_hour_of_day_scores: List[float],
        tower_cluster_radius: float = 1.0,
        tower_cluster_call_threshold: int = 0,
        subscriber_subset: Union[dict, None] = None,
    ):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.start_date = start_date
        self.stop_date = stop_date
        self.aggregation_unit = aggregation_unit
        self.label_a = label_a
        self.label_b = label_b
        self.labels = labels
        self.tower_day_of_week_scores = tower_day_of_week_scores
        self.tower_hour_of_day_scores = tower_hour_of_day_scores
        self.tower_cluster_radius = tower_cluster_radius
        self.tower_cluster_call_threshold = tower_cluster_call_threshold
        self.subscriber_subset = subscriber_subset

        common_params = dict(
            labels=labels,
            start_date=start_date,
            stop_date=stop_date,
            subscriber_subset=subscriber_subset,
            tower_cluster_call_threshold=tower_cluster_call_threshold,
            tower_cluster_radius=tower_cluster_radius,
            tower_day_of_week_scores=tower_day_of_week_scores,
            tower_hour_of_day_scores=tower_hour_of_day_scores,
        )
        locs_a = _make_meaningful_locations_object(label=label_a, **common_params)
        locs_b = _make_meaningful_locations_object(label=label_b, **common_params)

        self.q_meaningful_locations_od = MeaningfulLocationsOD(
            meaningful_locations_a=locs_a,
            meaningful_locations_b=locs_b,
            level=aggregation_unit,
        )

        super().__init__()  # NOTE: this *must* be called at the end of the __init__() method of any subclass of BaseExposedQuery

    @property
    def _flowmachine_query_obj(self):
        """
        Return the underlying flowmachine MeaningfulLocationsAggregate object.

        Returns
        -------
        MeaningfulLocationsOD
        """
        return self.q_meaningful_locations_od
