# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# TODO: Add __all__
from typing import Union, Dict, List, Optional, Tuple

from merge_args import merge_args

from flowclient import Connection
from flowclient.api_query import APIQuery


def location_event_counts_spec(
    *,
    start_date: str,
    end_date: str,
    aggregation_unit: str,
    count_interval: str,
    direction: str = "both",
    event_types: Optional[List[str]] = None,
    subscriber_subset: Optional[Union[dict, str]] = None,
    mapping_table: Optional[str] = None,
    geom_table: Optional[str] = None,
    geom_table_join_column: Optional[str] = None,
) -> dict:
    """
    Return query spec for a location event counts query aggregated spatially and temporally.
    Counts are taken over between 00:01 of start_date up until 00:00 of end_date (i.e. exclusive date range).

    Parameters
    ----------
    start_date : str
        ISO format date of the first day of the count, e.g. "2016-01-01"
    end_date : str
        ISO format date of the day _after_ the final date of the count, e.g. "2016-01-08"
    aggregation_unit : str
        Unit of aggregation, e.g. "admin3"
    count_interval : {"day", "hour", "minute"}
        Can be one of "day", "hour" or "minute".
    direction : {"in", "out", "both"}, default "both"
        Optionally, include only ingoing or outbound calls/texts. Can be one of "in", "out" or "both".
    event_types : list of {"calls", "sms", "mds", "topups"}, optional
        Optionally, include only a subset of event types (for example: ["calls", "sms"]).
        If None, include all event types in the query.
    subscriber_subset : dict or None, default None
        Subset of subscribers to include in event counts. Must be None
        (= all subscribers) or a dictionary with the specification of a
        subset query.

    Returns
    -------
    dict
        Dict which functions as the query specification
    """
    return {
        "query_kind": "location_event_counts",
        "start_date": start_date,
        "end_date": end_date,
        "interval": count_interval,
        "aggregation_unit": aggregation_unit,
        "direction": direction,
        "event_types": event_types,
        "subscriber_subset": subscriber_subset,
        "mapping_table": mapping_table,
        "geom_table": geom_table,
        "geom_table_join_column": geom_table_join_column,
    }


@merge_args(location_event_counts_spec)
def location_event_counts(*, connection: Connection, **kwargs) -> APIQuery:
    """
    Return a location event counts query aggregated spatially and temporally.
    Counts are taken over between 00:01 of start_date up until 00:00 of end_date (i.e. exclusive date range).

    Parameters
    ----------
    connection : Connection
        FlowKit API connection
    start_date : str
        ISO format date of the first day of the count, e.g. "2016-01-01"
    end_date : str
        ISO format date of the day _after_ the final date of the count, e.g. "2016-01-08"
    aggregation_unit : str
        Unit of aggregation, e.g. "admin3"
    count_interval : {"day", "hour", "minute"}
        Can be one of "day", "hour" or "minute".
    direction : {"in", "out", "both"}, default "both"
        Optionally, include only ingoing or outbound calls/texts. Can be one of "in", "out" or "both".
    event_types : list of {"calls", "sms", "mds", "topups"}, optional
        Optionally, include only a subset of event types (for example: ["calls", "sms"]).
        If None, include all event types in the query.
    subscriber_subset : dict or None, default None
        Subset of subscribers to include in event counts. Must be None
        (= all subscribers) or a dictionary with the specification of a
        subset query.

    Returns
    -------
    APIQuery
        Location event counts query
    """
    return connection.make_api_query(parameters=location_event_counts_spec(**kwargs))


def meaningful_locations_aggregate_spec(
    *,
    start_date: str,
    end_date: str,
    label: str,
    labels: Dict[str, Dict[str, dict]],
    tower_day_of_week_scores: Dict[str, float],
    tower_hour_of_day_scores: List[float],
    aggregation_unit: str,
    tower_cluster_radius: float = 1.0,
    tower_cluster_call_threshold: int = 0,
    event_types: Optional[List[str]] = None,
    subscriber_subset: Optional[Union[dict, str]] = None,
    mapping_table: Optional[str] = None,
    geom_table: Optional[str] = None,
    geom_table_join_column: Optional[str] = None,
) -> dict:
    """
    Return a query specification for a count of meaningful locations at some unit of spatial aggregation.
    Generates clusters of towers used by subscribers over the given time period, scores the clusters based on the
    subscribers' usage patterns over hours of the day and days of the week. Each subscriber then has a number of
    clusters, each of which has a score for hourly usage, and day of week usage. These clusters are then labelled
    based on whether they overlap with the regions of that space defined in the `labels` parameter.

    Once the clusters are labelled, those clusters which have the label specified are extracted, and then a count of
    subscribers per aggregation unit is returned, based on whether the _spatial_ position of the cluster overlaps with
    the aggregation unit. Subscribers are not counted directly, but contribute `1/number_of_clusters` to the count of
    each aggregation unit, for each cluster that lies within that aggregation unit.

    This methodology is based on work originally by Isaacman et al.[1]_, and extensions by Zagatti et al[2]_.

    Parameters
    ----------
    start_date : str
        ISO format date that begins the period, e.g. "2016-01-01"
    end_date : str
        ISO format date for the day _after_ the final date of the period, e.g. "2016-01-08"
    label : str
        One of the labels specified in `labels`, or 'unknown'. Locations with this
        label are returned.
    labels : dict of dicts
        A dictionary whose keys are the label names and the values geojson-style shapes,
        specified hour of day, and day of week score, with hour of day score on the x-axis
        and day of week score on the y-axis, where all scores are real numbers in the range [-1.0, +1.0]
    aggregation_unit : str
        Unit of aggregation, e.g. "admin3"
    tower_day_of_week_scores : dict
        A dictionary mapping days of the week ("monday", "tuesday" etc.) to numerical scores in the range [-1.0, +1.0].

        Each of a subscriber's interactions with a tower is given a score for the day of the week it took place on. For
        example, passing {"monday":1.0, "tuesday":0, "wednesday":0, "thursday":0, "friday":0, "saturday":0, "sunday":0}
        would score any interaction taking place on a monday 1, and 0 on all other days. So a subscriber who made two calls
        on a monday, and received one sms on tuesday, all from the same tower would have a final score of 0.666 for that
        tower.
    tower_hour_of_day_scores : list of float
        A length 24 list containing numerical scores in the range [-1.0, +1.0], where the first entry is midnight.
        Each of a subscriber's interactions with a tower is given a score for the hour of the day it took place in. For
        example, if the first entry of this list was 1, and all others were zero, each interaction the subscriber had
        that used a tower at midnight would receive a score of 1. If the subscriber used a particular tower twice, once
        at midnight, and once at noon, the final hour score for that tower would be 0.5.
    tower_cluster_radius : float
        When constructing clusters, towers will be considered for inclusion in a cluster only if they are within this
        number of km from the current cluster centroid. Hence, large values here will tend to produce clusters containing
        more towers, and fewer clusters.
    tower_cluster_call_threshold : int
        Exclude towers from a subscriber's clusters if they have been used on less than this number of days.
    event_types : list of {"calls", "sms", "mds", "topups"}, optional
        Optionally, include only a subset of event types (for example: ["calls", "sms"]).
        If None, include all event types in the query.
    subscriber_subset : dict or None
        Subset of subscribers to retrieve modal locations for. Must be None
        (= all subscribers), a dictionary with the specification of a
        subset query, or a string which is a valid query id.

    Returns
    -------
    dict
         Dict which functions as the query specification

    References
    ----------
    .. [1] S. Isaacman et al., "Identifying Important Places in People's Lives from Cellular Network Data", International Conference on Pervasive Computing (2011), pp 133-151.
    .. [2] Zagatti, Guilherme Augusto, et al. "A trip to work: Estimation of origin and destination of commuting patterns in the main metropolitan regions of Haiti using CDR." Development Engineering 3 (2018): 133-165.

    Notes
    -----
    Does not return any value below 15.
    """
    return {
        "query_kind": "meaningful_locations_aggregate",
        "aggregation_unit": aggregation_unit,
        "start_date": start_date,
        "end_date": end_date,
        "label": label,
        "labels": labels,
        "tower_day_of_week_scores": tower_day_of_week_scores,
        "tower_hour_of_day_scores": tower_hour_of_day_scores,
        "tower_cluster_radius": tower_cluster_radius,
        "tower_cluster_call_threshold": tower_cluster_call_threshold,
        "event_types": event_types,
        "subscriber_subset": subscriber_subset,
        "mapping_table": mapping_table,
        "geom_table": geom_table,
        "geom_table_join_column": geom_table_join_column,
    }


@merge_args(meaningful_locations_aggregate_spec)
def meaningful_locations_aggregate(*, connection: Connection, **kwargs) -> APIQuery:
    """
    Return a count of meaningful locations at some unit of spatial aggregation.
    Generates clusters of towers used by subscribers over the given time period, scores the clusters based on the
    subscribers' usage patterns over hours of the day and days of the week. Each subscriber then has a number of
    clusters, each of which has a score for hourly usage, and day of week usage. These clusters are then labelled
    based on whether they overlap with the regions of that space defined in the `labels` parameter.

    Once the clusters are labelled, those clusters which have the label specified are extracted, and then a count of
    subscribers per aggregation unit is returned, based on whether the _spatial_ position of the cluster overlaps with
    the aggregation unit. Subscribers are not counted directly, but contribute `1/number_of_clusters` to the count of
    each aggregation unit, for each cluster that lies within that aggregation unit.

    This methodology is based on work originally by Isaacman et al.[1]_, and extensions by Zagatti et al[2]_.

    Parameters
    ----------
    connection : Connection
        FlowKit API connection
    start_date : str
        ISO format date that begins the period, e.g. "2016-01-01"
    end_date : str
        ISO format date for the day _after_ the final date of the period, e.g. "2016-01-08"
    label : str
        One of the labels specified in `labels`, or 'unknown'. Locations with this
        label are returned.
    labels : dict of dicts
        A dictionary whose keys are the label names and the values geojson-style shapes,
        specified hour of day, and day of week score, with hour of day score on the x-axis
        and day of week score on the y-axis, where all scores are real numbers in the range [-1.0, +1.0]
    aggregation_unit : str
        Unit of aggregation, e.g. "admin3"
    tower_day_of_week_scores : dict
        A dictionary mapping days of the week ("monday", "tuesday" etc.) to numerical scores in the range [-1.0, +1.0].

        Each of a subscriber's interactions with a tower is given a score for the day of the week it took place on. For
        example, passing {"monday":1.0, "tuesday":0, "wednesday":0, "thursday":0, "friday":0, "saturday":0, "sunday":0}
        would score any interaction taking place on a monday 1, and 0 on all other days. So a subscriber who made two calls
        on a monday, and received one sms on tuesday, all from the same tower would have a final score of 0.666 for that
        tower.
    tower_hour_of_day_scores : list of float
        A length 24 list containing numerical scores in the range [-1.0, +1.0], where the first entry is midnight.
        Each of a subscriber's interactions with a tower is given a score for the hour of the day it took place in. For
        example, if the first entry of this list was 1, and all others were zero, each interaction the subscriber had
        that used a tower at midnight would receive a score of 1. If the subscriber used a particular tower twice, once
        at midnight, and once at noon, the final hour score for that tower would be 0.5.
    tower_cluster_radius : float
        When constructing clusters, towers will be considered for inclusion in a cluster only if they are within this
        number of km from the current cluster centroid. Hence, large values here will tend to produce clusters containing
        more towers, and fewer clusters.
    tower_cluster_call_threshold : int
        Exclude towers from a subscriber's clusters if they have been used on less than this number of days.
    event_types : list of {"calls", "sms", "mds", "topups"}, optional
        Optionally, include only a subset of event types (for example: ["calls", "sms"]).
        If None, include all event types in the query.
    subscriber_subset : dict or None
        Subset of subscribers to retrieve modal locations for. Must be None
        (= all subscribers), a dictionary with the specification of a
        subset query, or a string which is a valid query id.

    Returns
    -------
    APIQuery
        Meaningful locations aggregate query

    References
    ----------
    .. [1] S. Isaacman et al., "Identifying Important Places in People's Lives from Cellular Network Data", International Conference on Pervasive Computing (2011), pp 133-151.
    .. [2] Zagatti, Guilherme Augusto, et al. "A trip to work: Estimation of origin and destination of commuting patterns in the main metropolitan regions of Haiti using CDR." Development Engineering 3 (2018): 133-165.

    Notes
    -----
    Does not return any value below 15.
    """
    return connection.make_api_query(
        parameters=meaningful_locations_aggregate_spec(**kwargs)
    )


def meaningful_locations_between_label_od_matrix_spec(
    *,
    start_date: str,
    end_date: str,
    label_a: str,
    label_b: str,
    labels: Dict[str, Dict[str, dict]],
    tower_day_of_week_scores: Dict[str, float],
    tower_hour_of_day_scores: List[float],
    aggregation_unit: str,
    tower_cluster_radius: float = 1.0,
    tower_cluster_call_threshold: int = 0,
    event_types: Optional[List[str]] = None,
    subscriber_subset: Optional[Union[dict, str]] = None,
    mapping_table: Optional[str] = None,
    geom_table: Optional[str] = None,
    geom_table_join_column: Optional[str] = None,
) -> dict:
    """
    Return a query specification for an origin-destination matrix between two meaningful locations at some unit of spatial aggregation.
    Generates clusters of towers used by subscribers' over the given time period, scores the clusters based on the
    subscribers' usage patterns over hours of the day and days of the week. Each subscriber then has a number of
    clusters, each of which has a score for hourly usage, and day of week usage. These clusters are then labelled
    based on whether they overlap with the regions of that space defined in the `labels` parameter.

    Once the clusters are labelled, those clusters which have either `label_a` or `label_b` are extracted, and then
    a count of number of subscribers who move between the labels is returned, after aggregating spatially.
    Each subscriber contributes to `1/(num_cluster_with_label_a*num_clusters_with_label_b)` to the count. So, for example
    a subscriber with two clusters labelled evening, and one labelled day, all in different spatial units would contribute
    0.5 to the flow from each of the spatial units containing the evening clusters, to the unit containing the day cluster.

    This methodology is based on work originally by Isaacman et al.[1]_, and extensions by Zagatti et al[2]_.

    Parameters
    ----------
    start_date : str
        ISO format date that begins the period, e.g. "2016-01-01"
    end_date : str
        ISO format date for the day _after_ the final date of the period, e.g. "2016-01-08"
    label_a, label_b : str
        One of the labels specified in `labels`, or 'unknown'. Calculates the OD between these two labels.
    labels : dict of dicts
        A dictionary whose keys are the label names and the values geojson-style shapes,
        specified hour of day, and day of week score, with hour of day score on the x-axis
        and day of week score on the y-axis, where all scores are real numbers in the range [-1.0, +1.0]
    aggregation_unit : str
        Unit of aggregation, e.g. "admin3"
    tower_day_of_week_scores : dict
        A dictionary mapping days of the week ("monday", "tuesday" etc.) to numerical scores in the range [-1.0, +1.0].

        Each of a subscriber's interactions with a tower is given a score for the day of the week it took place on. For
        example, passing {"monday":1.0, "tuesday":0, "wednesday":0, "thursday":0, "friday":0, "saturday":0, "sunday":0}
        would score any interaction taking place on a monday 1, and 0 on all other days. So a subscriber who made two calls
        on a monday, and received one sms on tuesday, all from the same tower would have a final score of 0.666 for that
        tower.
    tower_hour_of_day_scores : list of float
        A length 24 list containing numerical scores in the range [-1.0, +1.0], where the first entry is midnight.
        Each of a subscriber's interactions with a tower is given a score for the hour of the day it took place in. For
        example, if the first entry of this list was 1, and all others were zero, each interaction the subscriber had
        that used a tower at midnight would receive a score of 1. If the subscriber used a particular tower twice, once
        at midnight, and once at noon, the final hour score for that tower would be 0.5.
    tower_cluster_radius : float
        When constructing clusters, towers will be considered for inclusion in a cluster only if they are within this
        number of km from the current cluster centroid. Hence, large values here will tend to produce clusters containing
        more towers, and fewer clusters.
    tower_cluster_call_threshold : int
        Exclude towers from a subscriber's clusters if they have been used on less than this number of days.
    event_types : list of {"calls", "sms", "mds", "topups"}, optional
        Optionally, include only a subset of event types (for example: ["calls", "sms"]).
        If None, include all event types in the query.
    subscriber_subset : dict or None
        Subset of subscribers to retrieve modal locations for. Must be None
        (= all subscribers), a dictionary with the specification of a
        subset query, or a string which is a valid query id.

    Returns
    -------
    dict
         Dict which functions as the query specification

    Notes
    -----
    Does not return any value below 15.

    References
    ----------
    .. [1] S. Isaacman et al., "Identifying Important Places in People's Lives from Cellular Network Data", International Conference on Pervasive Computing (2011), pp 133-151.
    .. [2] Zagatti, Guilherme Augusto, et al. "A trip to work: Estimation of origin and destination of commuting patterns in the main metropolitan regions of Haiti using CDR." Development Engineering 3 (2018): 133-165.
    """
    return {
        "query_kind": "meaningful_locations_between_label_od_matrix",
        "aggregation_unit": aggregation_unit,
        "start_date": start_date,
        "end_date": end_date,
        "label_a": label_a,
        "label_b": label_b,
        "labels": labels,
        "tower_day_of_week_scores": tower_day_of_week_scores,
        "tower_hour_of_day_scores": tower_hour_of_day_scores,
        "tower_cluster_radius": tower_cluster_radius,
        "tower_cluster_call_threshold": tower_cluster_call_threshold,
        "event_types": event_types,
        "subscriber_subset": subscriber_subset,
        "mapping_table": mapping_table,
        "geom_table": geom_table,
        "geom_table_join_column": geom_table_join_column,
    }


@merge_args(meaningful_locations_between_label_od_matrix_spec)
def meaningful_locations_between_label_od_matrix(
    *, connection: Connection, **kwargs
) -> APIQuery:
    """
    Return an origin-destination matrix between two meaningful locations at some unit of spatial aggregation.
    Generates clusters of towers used by subscribers' over the given time period, scores the clusters based on the
    subscribers' usage patterns over hours of the day and days of the week. Each subscriber then has a number of
    clusters, each of which has a score for hourly usage, and day of week usage. These clusters are then labelled
    based on whether they overlap with the regions of that space defined in the `labels` parameter.

    Once the clusters are labelled, those clusters which have either `label_a` or `label_b` are extracted, and then
    a count of number of subscribers who move between the labels is returned, after aggregating spatially.
    Each subscriber contributes to `1/(num_cluster_with_label_a*num_clusters_with_label_b)` to the count. So, for example
    a subscriber with two clusters labelled evening, and one labelled day, all in different spatial units would contribute
    0.5 to the flow from each of the spatial units containing the evening clusters, to the unit containing the day cluster.

    This methodology is based on work originally by Isaacman et al.[1]_, and extensions by Zagatti et al[2]_.

    Parameters
    ----------
    connection : Connection
        FlowKit API connection
    start_date : str
        ISO format date that begins the period, e.g. "2016-01-01"
    end_date : str
        ISO format date for the day _after_ the final date of the period, e.g. "2016-01-08"
    label_a, label_b : str
        One of the labels specified in `labels`, or 'unknown'. Calculates the OD between these two labels.
    labels : dict of dicts
        A dictionary whose keys are the label names and the values geojson-style shapes,
        specified hour of day, and day of week score, with hour of day score on the x-axis
        and day of week score on the y-axis, where all scores are real numbers in the range [-1.0, +1.0]
    aggregation_unit : str
        Unit of aggregation, e.g. "admin3"
    tower_day_of_week_scores : dict
        A dictionary mapping days of the week ("monday", "tuesday" etc.) to numerical scores in the range [-1.0, +1.0].

        Each of a subscriber's interactions with a tower is given a score for the day of the week it took place on. For
        example, passing {"monday":1.0, "tuesday":0, "wednesday":0, "thursday":0, "friday":0, "saturday":0, "sunday":0}
        would score any interaction taking place on a monday 1, and 0 on all other days. So a subscriber who made two calls
        on a monday, and received one sms on tuesday, all from the same tower would have a final score of 0.666 for that
        tower.
    tower_hour_of_day_scores : list of float
        A length 24 list containing numerical scores in the range [-1.0, +1.0], where the first entry is midnight.
        Each of a subscriber's interactions with a tower is given a score for the hour of the day it took place in. For
        example, if the first entry of this list was 1, and all others were zero, each interaction the subscriber had
        that used a tower at midnight would receive a score of 1. If the subscriber used a particular tower twice, once
        at midnight, and once at noon, the final hour score for that tower would be 0.5.
    tower_cluster_radius : float
        When constructing clusters, towers will be considered for inclusion in a cluster only if they are within this
        number of km from the current cluster centroid. Hence, large values here will tend to produce clusters containing
        more towers, and fewer clusters.
    tower_cluster_call_threshold : int
        Exclude towers from a subscriber's clusters if they have been used on less than this number of days.
    event_types : list of {"calls", "sms", "mds", "topups"}, optional
        Optionally, include only a subset of event types (for example: ["calls", "sms"]).
        If None, include all event types in the query.
    subscriber_subset : dict or None
        Subset of subscribers to retrieve modal locations for. Must be None
        (= all subscribers), a dictionary with the specification of a
        subset query, or a string which is a valid query id.

    Returns
    -------
    APIQuery
        Meaningful locations between label OD matrix query

    Notes
    -----
    Does not return any value below 15.

    References
    ----------
    .. [1] S. Isaacman et al., "Identifying Important Places in People's Lives from Cellular Network Data", International Conference on Pervasive Computing (2011), pp 133-151.
    .. [2] Zagatti, Guilherme Augusto, et al. "A trip to work: Estimation of origin and destination of commuting patterns in the main metropolitan regions of Haiti using CDR." Development Engineering 3 (2018): 133-165.
    """
    return connection.make_api_query(
        parameters=meaningful_locations_between_label_od_matrix_spec(**kwargs),
    )


def meaningful_locations_between_dates_od_matrix_spec(
    *,
    start_date_a: str,
    end_date_a: str,
    start_date_b: str,
    end_date_b: str,
    label: str,
    labels: Dict[str, Dict[str, dict]],
    tower_day_of_week_scores: Dict[str, float],
    tower_hour_of_day_scores: List[float],
    aggregation_unit: str,
    tower_cluster_radius: float = 1.0,
    tower_cluster_call_threshold: float = 0,
    event_types: Optional[List[str]] = None,
    subscriber_subset: Optional[Union[dict, str]] = None,
    mapping_table: Optional[str] = None,
    geom_table: Optional[str] = None,
    geom_table_join_column: Optional[str] = None,
) -> dict:
    """
    Return a query specification for an origin-destination matrix between one meaningful location in two time periods at some unit of spatial
    aggregation. This is analagous to performing a `flows` calculation.

    Generates clusters of towers used by subscribers' over the given time period, scores the clusters based on the
    subscribers' usage patterns over hours of the day and days of the week. Each subscriber then has a number of
    clusters, each of which has a score for hourly usage, and day of week usage. These clusters are then labelled
    based on whether they overlap with the regions of that space defined in the `labels` parameter.

    Once the clusters are labelled, those clusters which have a label of `label` are extracted, and then
    a count of of number of subscribers who's labelled clusters have moved between time periods is returned, after
    aggregating spatially.
    Each subscriber contributes to `1/(num_cluster_with_label_in_period_a*num_clusters_with_label_in_period_b)` to the
    count. So, for example a subscriber with two clusters labelled evening in the first time period, and only one in the
    second time period, with all clusters in different spatial units, would contribute 0.5 to the flow from the spatial
    units holding both the original clusters, to the spatial unit of the cluster in the second time period.

    This methodology is based on work originally by Isaacman et al.[1]_, and extensions by Zagatti et al[2]_.

    Parameters
    ----------
    start_date_a, start_date_b : str
        ISO format date that begins the period, e.g. "2016-01-01"
    end_date_a, end_date_b : str
        ISO format date for the day _after_ the final date of the period, e.g. "2016-01-08"
    label : str
        One of the labels specified in `labels`, or 'unknown'. Locations with this
        label are returned.
    labels : dict of dicts
        A dictionary whose keys are the label names and the values geojson-style shapes,
        specified hour of day, and day of week score, with hour of day score on the x-axis
        and day of week score on the y-axis, where all scores are real numbers in the range [-1.0, +1.0]
    aggregation_unit : str
        Unit of aggregation, e.g. "admin3"
    tower_day_of_week_scores : dict
        A dictionary mapping days of the week ("monday", "tuesday" etc.) to numerical scores in the range [-1.0, +1.0].

        Each of a subscriber's interactions with a tower is given a score for the day of the week it took place on. For
        example, passing {"monday":1.0, "tuesday":0, "wednesday":0, "thursday":0, "friday":0, "saturday":0, "sunday":0}
        would score any interaction taking place on a monday 1, and 0 on all other days. So a subscriber who made two calls
        on a monday, and received one sms on tuesday, all from the same tower would have a final score of 0.666 for that
        tower.
    tower_hour_of_day_scores : list of float
        A length 24 list containing numerical scores in the range [-1.0, +1.0], where the first entry is midnight.
        Each of a subscriber's interactions with a tower is given a score for the hour of the day it took place in. For
        example, if the first entry of this list was 1, and all others were zero, each interaction the subscriber had
        that used a tower at midnight would receive a score of 1. If the subscriber used a particular tower twice, once
        at midnight, and once at noon, the final hour score for that tower would be 0.5.
    tower_cluster_radius : float
        When constructing clusters, towers will be considered for inclusion in a cluster only if they are within this
        number of km from the current cluster centroid. Hence, large values here will tend to produce clusters containing
        more towers, and fewer clusters.
    tower_cluster_call_threshold : int
        Exclude towers from a subscriber's clusters if they have been used on less than this number of days.
    event_types : list of {"calls", "sms", "mds", "topups"}, optional
        Optionally, include only a subset of event types (for example: ["calls", "sms"]).
        If None, include all event types in the query.
    subscriber_subset : dict or None
        Subset of subscribers to retrieve modal locations for. Must be None
        (= all subscribers), a dictionary with the specification of a
        subset query, or a string which is a valid query id.

    Returns
    -------
    dict
         Dict which functions as the query specification

    Notes
    -----
    Does not return any value below 15.

    References
    ----------
    .. [1] S. Isaacman et al., "Identifying Important Places in People's Lives from Cellular Network Data", International Conference on Pervasive Computing (2011), pp 133-151.
    .. [2] Zagatti, Guilherme Augusto, et al. "A trip to work: Estimation of origin and destination of commuting patterns in the main metropolitan regions of Haiti using CDR." Development Engineering 3 (2018): 133-165.
    """
    return {
        "query_kind": "meaningful_locations_between_dates_od_matrix",
        "aggregation_unit": aggregation_unit,
        "start_date_a": start_date_a,
        "end_date_a": end_date_a,
        "start_date_b": start_date_b,
        "end_date_b": end_date_b,
        "label": label,
        "labels": labels,
        "tower_day_of_week_scores": tower_day_of_week_scores,
        "tower_hour_of_day_scores": tower_hour_of_day_scores,
        "tower_cluster_radius": tower_cluster_radius,
        "tower_cluster_call_threshold": tower_cluster_call_threshold,
        "event_types": event_types,
        "subscriber_subset": subscriber_subset,
        "mapping_table": mapping_table,
        "geom_table": geom_table,
        "geom_table_join_column": geom_table_join_column,
    }


@merge_args(meaningful_locations_between_dates_od_matrix_spec)
def meaningful_locations_between_dates_od_matrix(
    *, connection: Connection, **kwargs
) -> APIQuery:
    """
    Return an origin-destination matrix between one meaningful location in two time periods at some unit of spatial
    aggregation. This is analagous to performing a `flows` calculation.

    Generates clusters of towers used by subscribers' over the given time period, scores the clusters based on the
    subscribers' usage patterns over hours of the day and days of the week. Each subscriber then has a number of
    clusters, each of which has a score for hourly usage, and day of week usage. These clusters are then labelled
    based on whether they overlap with the regions of that space defined in the `labels` parameter.

    Once the clusters are labelled, those clusters which have a label of `label` are extracted, and then
    a count of of number of subscribers who's labelled clusters have moved between time periods is returned, after
    aggregating spatially.
    Each subscriber contributes to `1/(num_cluster_with_label_in_period_a*num_clusters_with_label_in_period_b)` to the
    count. So, for example a subscriber with two clusters labelled evening in the first time period, and only one in the
    second time period, with all clusters in different spatial units, would contribute 0.5 to the flow from the spatial
    units holding both the original clusters, to the spatial unit of the cluster in the second time period.

    This methodology is based on work originally by Isaacman et al.[1]_, and extensions by Zagatti et al[2]_.

    Parameters
    ----------
    connection : Connection
        FlowKit API connection
    start_date_a, start_date_b : str
        ISO format date that begins the period, e.g. "2016-01-01"
    end_date_a, end_date_b : str
        ISO format date for the day _after_ the final date of the period, e.g. "2016-01-08"
    label : str
        One of the labels specified in `labels`, or 'unknown'. Locations with this
        label are returned.
    labels : dict of dicts
        A dictionary whose keys are the label names and the values geojson-style shapes,
        specified hour of day, and day of week score, with hour of day score on the x-axis
        and day of week score on the y-axis, where all scores are real numbers in the range [-1.0, +1.0]
    aggregation_unit : str
        Unit of aggregation, e.g. "admin3"
    tower_day_of_week_scores : dict
        A dictionary mapping days of the week ("monday", "tuesday" etc.) to numerical scores in the range [-1.0, +1.0].

        Each of a subscriber's interactions with a tower is given a score for the day of the week it took place on. For
        example, passing {"monday":1.0, "tuesday":0, "wednesday":0, "thursday":0, "friday":0, "saturday":0, "sunday":0}
        would score any interaction taking place on a monday 1, and 0 on all other days. So a subscriber who made two calls
        on a monday, and received one sms on tuesday, all from the same tower would have a final score of 0.666 for that
        tower.
    tower_hour_of_day_scores : list of float
        A length 24 list containing numerical scores in the range [-1.0, +1.0], where the first entry is midnight.
        Each of a subscriber's interactions with a tower is given a score for the hour of the day it took place in. For
        example, if the first entry of this list was 1, and all others were zero, each interaction the subscriber had
        that used a tower at midnight would receive a score of 1. If the subscriber used a particular tower twice, once
        at midnight, and once at noon, the final hour score for that tower would be 0.5.
    tower_cluster_radius : float
        When constructing clusters, towers will be considered for inclusion in a cluster only if they are within this
        number of km from the current cluster centroid. Hence, large values here will tend to produce clusters containing
        more towers, and fewer clusters.
    tower_cluster_call_threshold : int
        Exclude towers from a subscriber's clusters if they have been used on less than this number of days.
    event_types : list of {"calls", "sms", "mds", "topups"}, optional
        Optionally, include only a subset of event types (for example: ["calls", "sms"]).
        If None, include all event types in the query.
    subscriber_subset : dict or None
        Subset of subscribers to retrieve values for. Must be None
        (= all subscribers), a dictionary with the specification of a
        subset query, or a string which is a valid query id.

    Returns
    -------
    APIQuery
        Meaningful locations between dates OD matrix query

    Notes
    -----
    Does not return any value below 15.

    References
    ----------
    .. [1] S. Isaacman et al., "Identifying Important Places in People's Lives from Cellular Network Data", International Conference on Pervasive Computing (2011), pp 133-151.
    .. [2] Zagatti, Guilherme Augusto, et al. "A trip to work: Estimation of origin and destination of commuting patterns in the main metropolitan regions of Haiti using CDR." Development Engineering 3 (2018): 133-165.
    """
    return connection.make_api_query(
        parameters=meaningful_locations_between_dates_od_matrix_spec(**kwargs),
    )


def flows_spec(
    *,
    from_location: Dict[str, Union[str, Dict[str, str]]],
    to_location: Dict[str, Union[str, Dict[str, str]]],
) -> dict:
    """
    Return query spec for flows between two locations.

    Parameters
    ----------
    from_location: dict
        Query which maps individuals to single location for the "origin" period of interest.
    to_location: dict
        Query which maps individuals to single location for the "destination" period of interest.

    Returns
    -------
    dict
        Dict which functions as the query specification for the flow

    """
    return {
        "query_kind": "flows",
        "from_location": from_location,
        "to_location": to_location,
    }


@merge_args(flows_spec)
def flows(*, connection: Connection, **kwargs) -> APIQuery:
    """
    Flows between two locations.

    Parameters
    ----------
    connection : Connection
        FlowKit API connection
    from_location: dict
        Query which maps individuals to single location for the "origin" period of interest.
    to_location: dict
        Query which maps individuals to single location for the "destination" period of interest.

    Returns
    -------
    APIQuery
        Flows query

    """
    return connection.make_api_query(parameters=flows_spec(**kwargs))


def unique_subscriber_counts_spec(
    *,
    start_date: str,
    end_date: str,
    aggregation_unit: str,
    mapping_table: Optional[str] = None,
    geom_table: Optional[str] = None,
    geom_table_join_column: Optional[str] = None,
    event_types: Optional[List[str]] = None,
    subscriber_subset: Optional[Union[dict, str]] = None,
) -> dict:
    """
    Return query spec for unique subscriber counts

    Parameters
    ----------
    start_date : str
        ISO format date of the first day of the count, e.g. "2016-01-01"
    end_date : str
        ISO format date of the day _after_ the final date of the count, e.g. "2016-01-08"
    aggregation_unit : str
        Unit of aggregation, e.g. "admin3"
    event_types : list of {"calls", "sms", "mds", "topups"}, optional
        Optionally, include only a subset of event types (for example: ["calls", "sms"]).
        If None, include all event types in the query.
    subscriber_subset : dict or None
        Subset of subscribers to retrieve values for. Must be None
        (= all subscribers), a dictionary with the specification of a
        subset query, or a string which is a valid query id.

    Returns
    -------
    dict
        Dict which functions as the query specification
    """
    return {
        "query_kind": "unique_subscriber_counts",
        "start_date": start_date,
        "end_date": end_date,
        "aggregation_unit": aggregation_unit,
        "mapping_table": mapping_table,
        "geom_table": geom_table,
        "geom_table_join_column": geom_table_join_column,
        "event_types": event_types,
        "subscriber_subset": subscriber_subset,
    }


@merge_args(unique_subscriber_counts_spec)
def unique_subscriber_counts(*, connection: Connection, **kwargs) -> APIQuery:
    """
    Return unique subscriber counts query

    Parameters
    ----------
    connection : Connection
        FlowKit API connection
    start_date : str
        ISO format date of the first day of the count, e.g. "2016-01-01"
    end_date : str
        ISO format date of the day _after_ the final date of the count, e.g. "2016-01-08"
    aggregation_unit : str
        Unit of aggregation, e.g. "admin3"
    event_types : list of {"calls", "sms", "mds", "topups"}, optional
        Optionally, include only a subset of event types (for example: ["calls", "sms"]).
        If None, include all event types in the query.
    subscriber_subset : dict or None
        Subset of subscribers to retrieve values for. Must be None
        (= all subscribers), a dictionary with the specification of a
        subset query, or a string which is a valid query id.

    Returns
    -------
    APIQuery
        Unique subscriber counts query
    """
    return connection.make_api_query(parameters=unique_subscriber_counts_spec(**kwargs))


def location_introversion_spec(
    *,
    start_date: str,
    end_date: str,
    aggregation_unit: str,
    direction: str = "both",
    mapping_table: Optional[str] = None,
    geom_table: Optional[str] = None,
    geom_table_join_column: Optional[str] = None,
    event_types: Optional[List[str]] = None,
    subscriber_subset: Optional[Union[dict, str]] = None,
) -> dict:
    """
    Return query spec for location introversion

    Parameters
    ----------
    start_date : str
        ISO format date of the first day of the count, e.g. "2016-01-01"
    end_date : str
        ISO format date of the day _after_ the final date of the count, e.g. "2016-01-08"
    aggregation_unit : str
        Unit of aggregation, e.g. "admin3"
    direction : {"in", "out", "both"}, default "both"
        Optionally, include only ingoing or outbound calls/texts can be one of "in", "out" or "both"
    event_types : list of {"calls", "sms", "mds", "topups"}, optional
        Optionally, include only a subset of event types (for example: ["calls", "sms"]).
        If None, include all event types in the query.
    subscriber_subset : dict or None
        Subset of subscribers to retrieve values for. Must be None
        (= all subscribers), a dictionary with the specification of a
        subset query, or a string which is a valid query id.

    Returns
    -------
    dict
        Dict which functions as the query specification
    """
    return {
        "query_kind": "location_introversion",
        "start_date": start_date,
        "end_date": end_date,
        "aggregation_unit": aggregation_unit,
        "direction": direction,
        "mapping_table": mapping_table,
        "geom_table": geom_table,
        "geom_table_join_column": geom_table_join_column,
        "event_types": event_types,
        "subscriber_subset": subscriber_subset,
    }


@merge_args(location_introversion_spec)
def location_introversion(*, connection: Connection, **kwargs) -> APIQuery:
    """
    Return location introversion query

    Parameters
    ----------
    connection : Connection
        FlowKit API connection
    start_date : str
        ISO format date of the first day of the count, e.g. "2016-01-01"
    end_date : str
        ISO format date of the day _after_ the final date of the count, e.g. "2016-01-08"
    aggregation_unit : str
        Unit of aggregation, e.g. "admin3"
    direction : {"in", "out", "both"}, default "both"
        Optionally, include only ingoing or outbound calls/texts can be one of "in", "out" or "both"
    event_types : list of {"calls", "sms", "mds", "topups"}, optional
        Optionally, include only a subset of event types (for example: ["calls", "sms"]).
        If None, include all event types in the query.
    subscriber_subset : dict or None
        Subset of subscribers to retrieve values for. Must be None
        (= all subscribers), a dictionary with the specification of a
        subset query, or a string which is a valid query id.

    Returns
    -------
    APIQuery
        Location introversion query
    """
    return connection.make_api_query(parameters=location_introversion_spec(**kwargs))


def total_network_objects_spec(
    *,
    start_date: str,
    end_date: str,
    aggregation_unit: str,
    total_by: str = "day",
    mapping_table: Optional[str] = None,
    geom_table: Optional[str] = None,
    geom_table_join_column: Optional[str] = None,
    event_types: Optional[List[str]] = None,
    subscriber_subset: Optional[Union[dict, str]] = None,
) -> dict:
    """
    Return query spec for total network objects

    Parameters
    ----------
    start_date : str
        ISO format date of the first day of the count, e.g. "2016-01-01"
    end_date : str
        ISO format date of the day _after_ the final date of the count, e.g. "2016-01-08"
    aggregation_unit : str
        Unit of aggregation, e.g. "admin3"
    total_by : {"second", "minute", "hour", "day", "month", "year"}
        Time period to bucket by one of "second", "minute", "hour", "day", "month" or "year"
    event_types : list of {"calls", "sms", "mds", "topups"}, optional
        Optionally, include only a subset of event types (for example: ["calls", "sms"]).
        If None, include all event types in the query.
    subscriber_subset : dict or None
        Subset of subscribers to retrieve values for. Must be None
        (= all subscribers), a dictionary with the specification of a
        subset query, or a string which is a valid query id.

    Returns
    -------
    dict
        Dict which functions as the query specification
    """
    return {
        "query_kind": "total_network_objects",
        "start_date": start_date,
        "end_date": end_date,
        "aggregation_unit": aggregation_unit,
        "total_by": total_by,
        "mapping_table": mapping_table,
        "geom_table": geom_table,
        "geom_table_join_column": geom_table_join_column,
        "event_types": event_types,
        "subscriber_subset": subscriber_subset,
    }


@merge_args(total_network_objects_spec)
def total_network_objects(*, connection: Connection, **kwargs) -> APIQuery:
    """
    Return total network objects query

    Parameters
    ----------
    connection : Connection
        FlowKit API connection
    start_date : str
        ISO format date of the first day of the count, e.g. "2016-01-01"
    end_date : str
        ISO format date of the day _after_ the final date of the count, e.g. "2016-01-08"
    aggregation_unit : str
        Unit of aggregation, e.g. "admin3"
    total_by : {"second", "minute", "hour", "day", "month", "year"}
        Time period to bucket by one of "second", "minute", "hour", "day", "month" or "year"
    event_types : list of {"calls", "sms", "mds", "topups"}, optional
        Optionally, include only a subset of event types (for example: ["calls", "sms"]).
        If None, include all event types in the query.
    subscriber_subset : dict or None
        Subset of subscribers to retrieve values for. Must be None
        (= all subscribers), a dictionary with the specification of a
        subset query, or a string which is a valid query id.

    Returns
    -------
    APIQuery
        Total network objects query
    """
    return connection.make_api_query(parameters=total_network_objects_spec(**kwargs))


def aggregate_network_objects_spec(
    *, total_network_objects: Dict[str, str], statistic: str, aggregate_by: str = "day"
) -> dict:
    """
    Return query spec for aggregate network objects

    Parameters
    ----------
    total_network_objects : dict
        Query spec produced by total_network_objects
    statistic : {"avg", "max", "min", "median", "mode", "stddev", "variance"}
        Statistic type one of "avg", "max", "min", "median", "mode", "stddev" or "variance".
    aggregate_by : {"second", "minute", "hour", "day", "month", "year", "century"}
        Period type one of "second", "minute", "hour", "day", "month", "year" or "century".

    Returns
    -------
    dict
        Query specification for an aggregated network objects query
    """
    total_network_objs = total_network_objects

    return {
        "query_kind": "aggregate_network_objects",
        "total_network_objects": total_network_objs,
        "statistic": statistic,
        "aggregate_by": aggregate_by,
    }


@merge_args(aggregate_network_objects_spec)
def aggregate_network_objects(*, connection: Connection, **kwargs) -> APIQuery:
    """
    Return aggregate network objects query

    Parameters
    ----------
    connection : Connection
        FlowKit API connection
    total_network_objects : dict
        Query spec produced by total_network_objects
    statistic : {"avg", "max", "min", "median", "mode", "stddev", "variance"}
        Statistic type one of "avg", "max", "min", "median", "mode", "stddev" or "variance".
    aggregate_by : {"second", "minute", "hour", "day", "month", "year", "century"}
        Period type one of "second", "minute", "hour", "day", "month", "year" or "century".

    Returns
    -------
    APIQuery
        Aggregate network objects query
    """
    return connection.make_api_query(
        parameters=aggregate_network_objects_spec(**kwargs)
    )


def spatial_aggregate_spec(*, locations: Dict[str, Union[str, Dict[str, str]]]) -> dict:
    """
    Return a query spec for a spatially aggregated modal or daily location.

    Parameters
    ----------
    locations : dict
        Modal or daily location query to aggregate spatially

    Returns
    -------
    dict
        Query specification for an aggregated daily or modal location
    """
    return {"query_kind": "spatial_aggregate", "locations": locations}


@merge_args(spatial_aggregate_spec)
def spatial_aggregate(*, connection: Connection, **kwargs) -> APIQuery:
    """
    Spatially aggregated modal or daily location.

    Parameters
    ----------
    connection : Connection
        FlowKit API connection
    locations : dict
        Modal or daily location query to aggregate spatially

    Returns
    -------
    APIQuery
        Spatial aggregate query
    """
    return connection.make_api_query(parameters=spatial_aggregate_spec(**kwargs))


def consecutive_trips_od_matrix_spec(
    *,
    start_date: str,
    end_date: str,
    aggregation_unit: str,
    event_types: Optional[List[str]] = None,
    subscriber_subset: Optional[Union[dict, str]] = None,
    mapping_table: Optional[str] = None,
    geom_table: Optional[str] = None,
    geom_table_join_column: Optional[str] = None,
) -> Dict[str, Union[str, Dict[str, str]]]:
    """
    Retrieves the count of subscriber who made consecutive visits between locations

    Parameters
    ----------
    start_date, end_date : str
        ISO format dates between which to find trips, e.g. "2016-01-01"
    aggregation_unit : str
        Unit of aggregation, e.g. "admin3"
    event_types : list of {"calls", "sms", "mds", "topups"}, optional
        Optionally, include only a subset of event types (for example: ["calls", "sms"]).
        If None, include all event types in the query.
    subscriber_subset : dict or None
        Subset of subscribers to retrieve trips for. Must be None
        (= all subscribers) or a dictionary with the specification of a
        subset query.

    Returns
    -------
    dict
        Consecutive trips od matrix query specification.

    """
    return dict(
        query_kind="consecutive_trips_od_matrix",
        start_date=start_date,
        end_date=end_date,
        aggregation_unit=aggregation_unit,
        event_types=event_types,
        subscriber_subset=subscriber_subset,
        mapping_table=mapping_table,
        geom_table=geom_table,
        geom_table_join_column=geom_table_join_column,
    )


@merge_args(consecutive_trips_od_matrix_spec)
def consecutive_trips_od_matrix(*, connection: Connection, **kwargs) -> APIQuery:
    """
    Retrieves the count of subscriber who made consecutive visits between locations

    Parameters
    ----------
    connection : Connection
        FlowKit API connection
    start_date, end_date : str
        ISO format dates between which to find trips, e.g. "2016-01-01"
    aggregation_unit : str
        Unit of aggregation, e.g. "admin3"
    event_types : list of {"calls", "sms", "mds", "topups"}, optional
        Optionally, include only a subset of event types (for example: ["calls", "sms"]).
        If None, include all event types in the query.
    subscriber_subset : dict or None
        Subset of subscribers to retrieve trips for. Must be None
        (= all subscribers) or a dictionary with the specification of a
        subset query.

    Returns
    -------
    APIQuery
        consecutive_trips_od_matrix query
    """
    return connection.make_api_query(
        parameters=consecutive_trips_od_matrix_spec(**kwargs),
    )


def trips_od_matrix_spec(
    *,
    start_date: str,
    end_date: str,
    aggregation_unit: str,
    event_types: Optional[List[str]] = None,
    subscriber_subset: Optional[Union[dict, str]] = None,
    mapping_table: Optional[str] = None,
    geom_table: Optional[str] = None,
    geom_table_join_column: Optional[str] = None,
) -> Dict[str, Union[str, Dict[str, str]]]:
    """
    Retrieves the count of subscriber who made visits between locations

    Parameters
    ----------
    start_date, end_date : str
        ISO format dates between which to find trips, e.g. "2016-01-01"
    aggregation_unit : str
        Unit of aggregation, e.g. "admin3"
    event_types : list of {"calls", "sms", "mds", "topups"}, optional
        Optionally, include only a subset of event types (for example: ["calls", "sms"]).
        If None, include all event types in the query.
    subscriber_subset : dict or None
        Subset of subscribers to retrieve trips for. Must be None
        (= all subscribers) or a dictionary with the specification of a
        subset query.

    Returns
    -------
    dict
        trips od matrix query specification.

    """
    return dict(
        query_kind="trips_od_matrix",
        start_date=start_date,
        end_date=end_date,
        aggregation_unit=aggregation_unit,
        event_types=event_types,
        subscriber_subset=subscriber_subset,
        mapping_table=mapping_table,
        geom_table=geom_table,
        geom_table_join_column=geom_table_join_column,
    )


@merge_args(trips_od_matrix_spec)
def trips_od_matrix(*, connection: Connection, **kwargs) -> APIQuery:
    """
    Retrieves the count of subscriber who made visits between locations

    Parameters
    ----------
    connection : Connection
        FlowKit API connection
    start_date, end_date : str
        ISO format dates between which to find trips, e.g. "2016-01-01"
    aggregation_unit : str
        Unit of aggregation, e.g. "admin3"
    event_types : list of {"calls", "sms", "mds", "topups"}, optional
        Optionally, include only a subset of event types (for example: ["calls", "sms"]).
        If None, include all event types in the query.
    subscriber_subset : dict or None
        Subset of subscribers to retrieve trips for. Must be None
        (= all subscribers) or a dictionary with the specification of a
        subset query.

    Returns
    -------
    APIQuery
        trips_od_matrix query
    """
    return connection.make_api_query(
        parameters=trips_od_matrix_spec(**kwargs),
    )


def unmoving_counts_spec(
    *,
    unique_locations: Dict[str, Union[str, Dict[str, str]]],
) -> Dict[str, Union[str, Dict[str, str]]]:
    """
    A count by location of subscribers who were unmoving at that location.

    Parameters
    ----------
    unique_locations : dict
        unique locations

    Returns
    -------
    dict
        Query specification

    """
    return dict(
        query_kind="unmoving_counts",
        locations=unique_locations,
    )


@merge_args(unmoving_counts_spec)
def unmoving_counts(*, connection: Connection, **kwargs) -> APIQuery:
    """
    A count by location of subscribers who were unmoving at that location.

    Parameters
    ----------
    connection : Connection
        FlowKit API connection
    unique_locations : dict
        unique locations

    Returns
    -------
    APIQuery
        unmoving_counts query
    """
    return connection.make_api_query(
        parameters=unmoving_counts_spec(**kwargs),
    )


def unmoving_at_reference_location_counts_spec(
    *,
    reference_locations: Dict[str, Union[str, Dict[str, str]]],
    unique_locations: Dict[str, Union[str, Dict[str, str]]],
) -> Dict[str, Union[str, Dict[str, str]]]:
    """
    A count by location of subscribers who were unmoving in their reference location.

    Parameters
    ----------
    reference_locations : dict
        Modal or daily location
    unique_locations : dict
        unique locations

    Returns
    -------
    dict
        Query specification

    """
    return dict(
        query_kind="unmoving_at_reference_location_counts",
        locations=unique_locations,
        reference_locations=reference_locations,
    )


@merge_args(unmoving_at_reference_location_counts_spec)
def unmoving_at_reference_location_counts(
    *, connection: Connection, **kwargs
) -> APIQuery:
    """
    A count by location of subscribers who were unmoving in their reference location.

    Parameters
    ----------
    connection : Connection
        FlowKit API connection
    reference_locations : dict
        Modal or daily location
    unique_locations : dict
        unique locations

    Returns
    -------
    APIQuery
        unmoving_at_reference_location_counts query
    """
    return connection.make_api_query(
        parameters=unmoving_at_reference_location_counts_spec(**kwargs),
    )


def active_at_reference_location_counts_spec(
    *,
    reference_locations: Dict[str, Union[str, Dict[str, str]]],
    unique_locations: Dict[str, Union[str, Dict[str, str]]],
) -> Dict[str, Union[str, Dict[str, str]]]:
    """
    A count by location of subscribers who were active in their reference location.

    Parameters
    ----------
    reference_locations : dict
        Modal or daily location
    unique_locations : dict
        unique locations

    Returns
    -------
    dict
        Query specification

    """
    return dict(
        query_kind="active_at_reference_location_counts",
        unique_locations=unique_locations,
        reference_locations=reference_locations,
    )


@merge_args(active_at_reference_location_counts_spec)
def active_at_reference_location_counts(
    *, connection: Connection, **kwargs
) -> APIQuery:
    """
    A count by location of subscribers who were active in their reference location.

    Parameters
    ----------
    connection : Connection
        FlowKit API connection
    reference_locations : dict
        Modal or daily location
    unique_locations : dict
        unique locations

    Returns
    -------
    APIQuery
        active_at_reference_location_counts query
    """
    return connection.make_api_query(
        parameters=active_at_reference_location_counts_spec(**kwargs),
    )


def joined_spatial_aggregate_spec(
    *,
    locations: Dict[str, Union[str, Dict[str, str]]],
    metric: Dict[str, Union[str, Dict[str, str]]],
    method: str = "avg",
) -> dict:
    """
    Return a query spec for a metric aggregated by attaching location information.

    Parameters
    ----------
    locations : dict
        Modal or daily location query to use to localise the metric
    metric: dict
        Metric to calculate and aggregate
    method: {"avg", "max", "min", "median", "mode", "stddev", "variance", "distr"}, default "avg".
       Method of aggregation; one of "avg", "max", "min", "median", "mode", "stddev", "variance" or "distr". If the metric refers to a categorical variable (e.g. a subscriber handset type) it will only accept the "distr" method which yields the relative distribution of possible values. All of the other methods will be rejected. On the other hand, the "distr" method will be rejected for all continuous variables.

    Returns
    -------
    dict

        Query specification for an aggregated daily or modal location
    """
    return {
        "query_kind": "joined_spatial_aggregate",
        "method": method,
        "locations": locations,
        "metric": metric,
    }


@merge_args(joined_spatial_aggregate_spec)
def joined_spatial_aggregate(*, connection: Connection, **kwargs) -> APIQuery:
    """
    Query for a metric aggregated by attaching location information.

    Parameters
    ----------
    connection : Connection
        FlowKit API connection
    locations : dict
        Modal or daily location query to use to localise the metric
    metric: dict
        Metric to calculate and aggregate
    method: {"avg", "max", "min", "median", "mode", "stddev", "variance", "distr"}, default "avg".
       Method of aggregation; one of "avg", "max", "min", "median", "mode", "stddev", "variance" or "distr". If the metric refers to a categorical variable (e.g. a subscriber handset type) it will only accept the "distr" method which yields the relative distribution of possible values. All of the other methods will be rejected. On the other hand, the "distr" method will be rejected for all continuous variables.

    Returns
    -------
    APIQuery
        Joined spatial aggregate query
    """
    return connection.make_api_query(parameters=joined_spatial_aggregate_spec(**kwargs))


def histogram_aggregate_spec(
    *,
    metric: Dict[str, Union[str, Dict[str, str]]],
    bins: Union[int, List[float]],
    range: Optional[Tuple[float, float]] = None,
) -> dict:
    """
    Return a query spec for a metric aggregated as a histogram.

    Parameters
    ----------
    metric : dict
        Metric to calculate and aggregate
    bins : int, or list of floats
       Either an integer number of bins for equally spaced bins, or a list of floats giving the lower and upper edges
    range : tuple of floats or None, default None
        Optionally supply inclusive lower and upper bounds to build the histogram over. By default, the
        histogram will cover the whole range of the data.

    Returns
    -------
    dict

        Query specification for histogram aggregate over a metric
    """
    if isinstance(bins, list):
        bins = dict(bin_list=bins)
    else:
        bins = dict(n_bins=bins)
    spec = dict(query_kind="histogram_aggregate", metric=metric, bins=bins)
    if range is not None:
        spec["range"] = list(range[:2])
    return spec


@merge_args(histogram_aggregate_spec)
def histogram_aggregate(*, connection: Connection, **kwargs) -> APIQuery:
    """
    Return a query spec for a metric aggregated as a histogram.

    Parameters
    ----------
    metric : dict
        Metric to calculate and aggregate
    bins : int, or list of floats
       Either an integer number of bins for equally spaced bins, or a list of floats giving the lower and upper edges
    range : tuple of floats or None, default None
        Optionally supply inclusive lower and upper bounds to build the histogram over. By default, the
        histogram will cover the whole range of the data.

    Returns
    -------
    APIQuery
        Histogram aggregate query
    """
    return connection.make_api_query(parameters=histogram_aggregate_spec(**kwargs))
