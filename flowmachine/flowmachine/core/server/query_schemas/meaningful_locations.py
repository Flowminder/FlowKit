from marshmallow import Schema, fields, post_load
from marshmallow.validate import OneOf, Range
from typing import Union, Dict, List

from flowmachine.features import (
    MeaningfulLocationsAggregate,
    MeaningfulLocations,
    HartiganCluster,
    CallDays,
    EventScore,
    subscriber_locations,
)
from .base_exposed_query import BaseExposedQuery


class MeaningfulLocationsSchema(Schema):
    start_date = fields.Date(required=True)
    stop_date = fields.Date(required=True)
    aggregation_unit = fields.String(
        validate=OneOf(["admin0", "admin1", "admin2", "admin3"])
    )
    label = fields.String(required=True)
    labels = fields.Dict(
        keys=fields.String(), values=fields.Dict()
    )  # TODO: use custom field here for stricter validation!
    tower_hour_of_day_scores = fields.List(
        fields.Float(validate=Range(min=-1.0, max=1.0))
    )
    tower_day_of_week_scores = fields.Dict(
        keys=fields.String(
            validate=OneOf(
                [
                    "monday",
                    "tuesday",
                    "wednesday",
                    "thursday",
                    "friday",
                    "saturday",
                    "sunday",
                ]
            )
        ),
        values=fields.Float(validate=Range(min=-1.0, max=1.0)),
    )
    tower_cluster_radius = fields.Float()


class MeaningfulLocationsAggregateSchema(Schema):
    start_date = fields.Date(required=True)
    stop_date = fields.Date(required=True)
    label = fields.String(required=True)
    labels = fields.Dict(
        keys=fields.String(), values=fields.Dict()
    )  # TODO: use custom field here for stricter validation!
    tower_hour_of_day_scores = fields.List(
        fields.Float(validate=Range(min=-1.0, max=1.0))
    )
    tower_day_of_week_scores = fields.Dict(
        keys=fields.String(
            validate=OneOf(
                [
                    "monday",
                    "tuesday",
                    "wednesday",
                    "thursday",
                    "friday",
                    "saturday",
                    "sunday",
                ]
            )
        ),
        values=fields.Float(validate=Range(min=-1.0, max=1.0)),
    )
    aggregation_unit = fields.String(
        validate=OneOf(["admin0", "admin1", "admin2", "admin3"])
    )
    tower_cluster_radius = fields.Float(required=False, default=1.0)
    tower_cluster_call_threshold: fields.Integer(required=False, default=0)
    subscriber_subset: fields.String(
        required=False, allow_none=True, validate=OneOf([None])
    )

    @post_load
    def make_query_object(self, params):
        return MeaningfulLocationsAggregateExposed(**params)


def _make_meaningful_locations_aggregate(
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
    q_subscriber_locations = subscriber_locations(
        start=start_date,
        stop=stop_date,
        level="versioned-site",
        subscriber_subset=subscriber_subset,
    )
    q_call_days = CallDays(subscriber_locations=q_subscriber_locations)
    q_hartigan_cluster = HartiganCluster(
        calldays=q_call_days,
        radius=tower_cluster_radius,
        call_threshold=tower_cluster_call_threshold,
        buffer=0,  # we're not exposing 'buffer', apparently
    )
    q_event_score = EventScore(
        start=start_date,
        stop=stop_date,
        score_hour=tower_hour_of_day_scores,
        score_dow=tower_day_of_week_scores,
        level="versioned-site",
        subscriber_subset=subscriber_subset,
    )
    q_meaningful_locations = MeaningfulLocations(
        clusters=q_hartigan_cluster, labels=labels, scores=q_event_score, label=label
    )

    return MeaningfulLocationsAggregateExposed(
        meaningful_locations=q_meaningful_locations, level=aggregation_unit
    )


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
            clusters=q_hartigan_cluster,
            labels=labels,
            scores=q_event_score,
            label=label,
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
