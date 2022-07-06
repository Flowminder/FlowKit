# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from marshmallow import fields
from marshmallow.validate import OneOf
from typing import Union, Dict, List, Optional

from flowmachine.core import make_spatial_unit
from flowmachine.features import (
    MeaningfulLocations,
    HartiganCluster,
    CallDays,
    EventScore,
    SubscriberLocations,
)
from flowmachine.features.location.meaningful_locations_aggregate import (
    MeaningfulLocationsAggregate,
)
from flowmachine.features.location.meaningful_locations_od import MeaningfulLocationsOD
from flowmachine.features.location.redacted_meaningful_locations_aggregate import (
    RedactedMeaningfulLocationsAggregate,
)
from flowmachine.features.location.redacted_meaningful_locations_od import (
    RedactedMeaningfulLocationsOD,
)
from .base_exposed_query import BaseExposedQuery
from .field_mixins import StartAndEndField, EventTypesField, SubscriberSubsetField
from .base_schema import BaseSchema
from .custom_fields import (
    TowerHourOfDayScores,
    TowerDayOfWeekScores,
    ISODateTime,
)
from .aggregation_unit import AggregationUnitMixin

__all__ = [
    "MeaningfulLocationsAggregateSchema",
    "MeaningfulLocationsAggregateExposed",
    "MeaningfulLocationsBetweenLabelODMatrixSchema",
    "MeaningfulLocationsBetweenLabelODMatrixExposed",
    "MeaningfulLocationsBetweenDatesODMatrixSchema",
    "MeaningfulLocationsBetweenDatesODMatrixExposed",
]

from ...spatial_unit import AnySpatialUnit


class MeaningfulLocationsAggregateExposed(BaseExposedQuery):
    # query_kind class attribute is required for nesting and serialisation
    query_kind = "meaningful_locations_aggregate"

    def __init__(
        self,
        *,
        start_date: str,
        end_date: str,
        aggregation_unit: AnySpatialUnit,
        label: str,
        labels: Dict[str, Dict[str, dict]],
        tower_day_of_week_scores: Dict[str, float],
        tower_hour_of_day_scores: List[float],
        tower_cluster_radius: float,
        tower_cluster_call_threshold: int,
        event_types: Optional[Union[str, List[str]]],
        subscriber_subset: Union[dict, None] = None,
    ):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.start_date = start_date
        self.end_date = end_date
        self.aggregation_unit = aggregation_unit
        self.event_types = event_types
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
            end_date=end_date,
            event_types=event_types,
            subscriber_subset=subscriber_subset,
            tower_cluster_call_threshold=tower_cluster_call_threshold,
            tower_cluster_radius=tower_cluster_radius,
            tower_day_of_week_scores=tower_day_of_week_scores,
            tower_hour_of_day_scores=tower_hour_of_day_scores,
        )
        self.q_meaningful_locations_aggregate = RedactedMeaningfulLocationsAggregate(
            meaningful_locations_aggregate=MeaningfulLocationsAggregate(
                meaningful_locations=q_meaningful_locations,
                spatial_unit=aggregation_unit,
            )
        )

    @property
    def _flowmachine_query_obj(self):
        """
        Return the underlying flowmachine MeaningfulLocationsAggregate object.

        Returns
        -------
        ModalLocation
        """
        return self.q_meaningful_locations_aggregate


class MeaningfulLocationsBetweenLabelODMatrixExposed(BaseExposedQuery):
    # query_kind class attribute is required for nesting and serialisation
    query_kind = "meaningful_locations_between_label_od_matrix"

    def __init__(
        self,
        *,
        start_date: str,
        end_date: str,
        aggregation_unit: AnySpatialUnit,
        label_a: str,
        label_b: str,
        labels: Dict[str, Dict[str, dict]],
        tower_day_of_week_scores: Dict[str, float],
        tower_hour_of_day_scores: List[float],
        tower_cluster_radius: float,
        tower_cluster_call_threshold: int,
        event_types: Optional[Union[str, List[str]]],
        subscriber_subset: Union[dict, None] = None,
    ):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.start_date = start_date
        self.end_date = end_date
        self.aggregation_unit = aggregation_unit
        self.event_types = event_types
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
            end_date=end_date,
            event_types=event_types,
            subscriber_subset=subscriber_subset,
            tower_cluster_call_threshold=tower_cluster_call_threshold,
            tower_cluster_radius=tower_cluster_radius,
            tower_day_of_week_scores=tower_day_of_week_scores,
            tower_hour_of_day_scores=tower_hour_of_day_scores,
        )
        locs_a = _make_meaningful_locations_object(label=label_a, **common_params)
        locs_b = _make_meaningful_locations_object(label=label_b, **common_params)

        self.q_meaningful_locations_od = RedactedMeaningfulLocationsOD(
            meaningful_locations_od=MeaningfulLocationsOD(
                meaningful_locations_a=locs_a,
                meaningful_locations_b=locs_b,
                spatial_unit=aggregation_unit,
            )
        )

    @property
    def _flowmachine_query_obj(self):
        """
        Return the underlying flowmachine MeaningfulLocationsAggregate object.

        Returns
        -------
        MeaningfulLocationsOD
        """
        return self.q_meaningful_locations_od


class MeaningfulLocationsBetweenDatesODMatrixExposed(BaseExposedQuery):
    # query_kind class attribute is required for nesting and serialisation
    query_kind = "meaningful_locations_between_dates_od_matrix"

    def __init__(
        self,
        *,
        start_date_a: str,
        end_date_a: str,
        start_date_b: str,
        end_date_b: str,
        aggregation_unit: AnySpatialUnit,
        label: str,
        labels: Dict[str, Dict[str, dict]],
        tower_day_of_week_scores: Dict[str, float],
        tower_hour_of_day_scores: List[float],
        tower_cluster_radius: float,
        tower_cluster_call_threshold: int,
        event_types: Optional[Union[str, List[str]]],
        subscriber_subset: Union[dict, None] = None,
    ):
        # Note: all input parameters need to be defined as attributes on `self`
        # so that marshmallow can serialise the object correctly.
        self.start_date_a = start_date_a
        self.start_date_b = start_date_b
        self.end_date_a = end_date_a
        self.end_date_b = end_date_b
        self.aggregation_unit = aggregation_unit
        self.event_types = event_types
        self.label = label
        self.labels = labels
        self.tower_day_of_week_scores = tower_day_of_week_scores
        self.tower_hour_of_day_scores = tower_hour_of_day_scores
        self.tower_cluster_radius = tower_cluster_radius
        self.tower_cluster_call_threshold = tower_cluster_call_threshold
        self.subscriber_subset = subscriber_subset

        common_params = dict(
            labels=labels,
            label=label,
            event_types=event_types,
            subscriber_subset=subscriber_subset,
            tower_cluster_call_threshold=tower_cluster_call_threshold,
            tower_cluster_radius=tower_cluster_radius,
            tower_day_of_week_scores=tower_day_of_week_scores,
            tower_hour_of_day_scores=tower_hour_of_day_scores,
        )
        locs_a = _make_meaningful_locations_object(
            start_date=start_date_a, end_date=end_date_a, **common_params
        )
        locs_b = _make_meaningful_locations_object(
            start_date=start_date_b, end_date=end_date_b, **common_params
        )

        self.q_meaningful_locations_od = MeaningfulLocationsOD(
            meaningful_locations_a=locs_a,
            meaningful_locations_b=locs_b,
            spatial_unit=aggregation_unit,
        )

    @property
    def _flowmachine_query_obj(self):
        """
        Return the underlying flowmachine MeaningfulLocationsAggregate object.

        Returns
        -------
        MeaningfulLocationsOD
        """
        return self.q_meaningful_locations_od


class MeaningfulLocationsAggregateSchema(
    StartAndEndField,
    EventTypesField,
    SubscriberSubsetField,
    AggregationUnitMixin,
    BaseSchema,
):
    __model__ = MeaningfulLocationsAggregateExposed

    # query_kind parameter is required here for claims validation
    query_kind = fields.String(validate=OneOf([__model__.query_kind]), required=True)
    label = fields.String(required=True)
    labels = fields.Dict(
        required=True, keys=fields.String(), values=fields.Dict()
    )  # TODO: use custom field here for stricter validation!
    tower_hour_of_day_scores = TowerHourOfDayScores(required=True)
    tower_day_of_week_scores = TowerDayOfWeekScores(required=True)
    tower_cluster_radius = fields.Float(required=False, load_default=1.0)
    tower_cluster_call_threshold = fields.Integer(required=False, load_default=0)


def _make_meaningful_locations_object(
    *,
    start_date,
    end_date,
    label,
    labels,
    event_types,
    subscriber_subset,
    tower_cluster_call_threshold,
    tower_cluster_radius,
    tower_day_of_week_scores,
    tower_hour_of_day_scores,
):
    q_subscriber_locations = SubscriberLocations(
        start=start_date,
        stop=end_date,
        spatial_unit=make_spatial_unit(
            "versioned-site"
        ),  # note this 'spatial_unit' is not the same as the exposed parameter 'aggregation_unit'
        table=event_types,
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
        stop=end_date,
        score_hour=tower_hour_of_day_scores,
        score_dow=tower_day_of_week_scores,
        spatial_unit=make_spatial_unit(
            "versioned-site"
        ),  # note this 'spatial_unit' is not the same as the exposed parameter 'aggregation_unit'
        table=event_types,
        subscriber_subset=subscriber_subset,
    )
    q_meaningful_locations = MeaningfulLocations(
        clusters=q_hartigan_cluster, labels=labels, scores=q_event_score, label=label
    )
    return q_meaningful_locations


class MeaningfulLocationsBetweenLabelODMatrixSchema(
    StartAndEndField,
    EventTypesField,
    SubscriberSubsetField,
    AggregationUnitMixin,
    BaseSchema,
):
    __model__ = MeaningfulLocationsBetweenLabelODMatrixExposed

    # query_kind parameter is required here for claims validation
    query_kind = fields.String(validate=OneOf([__model__.query_kind]), required=True)
    label_a = fields.String(required=True)
    label_b = fields.String(required=True)
    labels = fields.Dict(
        required=True, keys=fields.String(), values=fields.Dict()
    )  # TODO: use custom field here for stricter validation!
    tower_hour_of_day_scores = TowerHourOfDayScores(required=True)
    tower_day_of_week_scores = TowerDayOfWeekScores(required=True)
    tower_cluster_radius = fields.Float(required=False, load_default=1.0)
    tower_cluster_call_threshold = fields.Integer(required=False, load_default=0)


class MeaningfulLocationsBetweenDatesODMatrixSchema(
    EventTypesField, SubscriberSubsetField, AggregationUnitMixin, BaseSchema
):
    __model__ = MeaningfulLocationsBetweenDatesODMatrixExposed

    # query_kind parameter is required here for claims validation
    query_kind = fields.String(validate=OneOf([__model__.query_kind]), required=True)
    start_date_a = ISODateTime(required=True)
    end_date_a = ISODateTime(required=True)
    start_date_b = ISODateTime(required=True)
    end_date_b = ISODateTime(required=True)
    label = fields.String(required=True)
    labels = fields.Dict(
        required=True, keys=fields.String(), values=fields.Dict()
    )  # TODO: use custom field here for stricter validation!
    tower_hour_of_day_scores = TowerHourOfDayScores(required=True)
    tower_day_of_week_scores = TowerDayOfWeekScores(required=True)
    tower_cluster_radius = fields.Float(required=False, load_default=1.0)
    tower_cluster_call_threshold = fields.Integer(required=False, load_default=0)
