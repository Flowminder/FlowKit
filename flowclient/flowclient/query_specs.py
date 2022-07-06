# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from typing import Union, List, Dict, Optional, Tuple, Any

import pandas as pd


def unique_locations_spec(
    *,
    start_date: str,
    end_date: str,
    aggregation_unit: str,
    event_types: Optional[List[str]] = None,
    subscriber_subset: Optional[Union[dict, str]] = None,
    mapping_table: Optional[str] = None,
    geom_table: Optional[str] = None,
    geom_table_join_column: Optional[str] = None,
    hours: Optional[Tuple[int, int]] = None,
) -> dict:
    """
    Subscriber level query which retrieves the unique set of locations visited by each subscriber
    in the time period.

    Parameters
    ----------
    start_date, end_date : str
        ISO format dates between which to get unique locations, e.g. "2016-01-01"
    aggregation_unit : str
        Unit of aggregation, e.g. "admin3"
    event_types : list of {"calls", "sms", "mds", "topups"}, optional
        Optionally, include only a subset of event types (for example: ["calls", "sms"]).
        If None, include all event types in the query.
    subscriber_subset : dict or None
        Subset of subscribers to retrieve daily locations for. Must be None
        (= all subscribers) or a dictionary with the specification of a
        subset query.
    hours : tuple of int or None
        Hours of the day to include in query

    Returns
    -------
    dict
        Unique locations query specification.

    """
    return dict(
        query_kind="unique_locations",
        start_date=start_date,
        end_date=end_date,
        aggregation_unit=aggregation_unit,
        event_types=event_types,
        subscriber_subset=subscriber_subset,
        mapping_table=mapping_table,
        geom_table=geom_table,
        geom_table_join_column=geom_table_join_column,
        hours=None if hours is None else dict(start_hour=hours[0], end_hour=hours[1]),
    )


def most_frequent_location_spec(
    *,
    start_date: str,
    end_date: str,
    aggregation_unit: str,
    hours: Optional[Tuple[int, int]] = None,
    event_types: Optional[List[str]] = None,
    subscriber_subset: Optional[Union[dict, str]] = None,
    mapping_table: Optional[str] = None,
    geom_table: Optional[str] = None,
    geom_table_join_column: Optional[str] = None,
) -> dict:
    """
    Subscriber level query which retrieves the location most frequently visited by each subscriber
    in the time period.

    Parameters
    ----------
    start_date, end_date : str
        ISO format dates between which to get locations, e.g. "2016-01-01"
    hours : tuple of int
        Hours of the day to include locations from 0-24.
    aggregation_unit : str
        Unit of aggregation, e.g. "admin3"
    event_types : list of {"calls", "sms", "mds", "topups"}, optional
        Optionally, include only a subset of event types (for example: ["calls", "sms"]).
        If None, include all event types in the query.
    subscriber_subset : dict or None
        Subset of subscribers to retrieve locations for. Must be None
        (= all subscribers) or a dictionary with the specification of a
        subset query.

    Returns
    -------
    dict
        Most frequent location query specification.

    """
    return dict(
        query_kind="most_frequent_location",
        start_date=start_date,
        end_date=end_date,
        hours=None if hours is None else dict(start_hour=hours[0], end_hour=hours[1]),
        aggregation_unit=aggregation_unit,
        event_types=event_types,
        subscriber_subset=subscriber_subset,
        mapping_table=mapping_table,
        geom_table=geom_table,
        geom_table_join_column=geom_table_join_column,
    )


def daily_location_spec(
    *,
    date: str,
    aggregation_unit: str,
    method: str,
    event_types: Optional[List[str]] = None,
    subscriber_subset: Optional[Union[dict, str]] = None,
    mapping_table: Optional[str] = None,
    geom_table: Optional[str] = None,
    geom_table_join_column: Optional[str] = None,
    hours: Optional[Tuple[int, int]] = None,
) -> dict:
    """
    Return query spec for a daily location query for a date and unit of aggregation.
    Must be passed to `spatial_aggregate` to retrieve a result from the aggregates API.

    Parameters
    ----------
    date : str
        ISO format date to get the daily location for, e.g. "2016-01-01"
    aggregation_unit : str
        Unit of aggregation, e.g. "admin3"
    method : str
        Method to use for daily location, one of 'last' or 'most-common'
    event_types : list of {"calls", "sms", "mds", "topups"}, optional
        Optionally, include only a subset of event types (for example: ["calls", "sms"]).
        If None, include all event types in the query.
    subscriber_subset : dict or None
        Subset of subscribers to retrieve daily locations for. Must be None
        (= all subscribers) or a dictionary with the specification of a
        subset query.
    hours : tuple of int
        Hours of the day to include

    Returns
    -------
    dict
        Dict which functions as the query specification

    """
    return {
        "query_kind": "daily_location",
        "date": date,
        "aggregation_unit": aggregation_unit,
        "method": method,
        "event_types": event_types,
        "subscriber_subset": subscriber_subset,
        "mapping_table": mapping_table,
        "geom_table": geom_table,
        "geom_table_join_column": geom_table_join_column,
        "hours": None
        if hours is None
        else dict(start_hour=hours[0], end_hour=hours[1]),
    }


def modal_location_spec(
    *, locations: List[Dict[str, Union[str, Dict[str, str]]]]
) -> dict:
    """
    Return query spec for a modal location query for a list of locations.
    Must be passed to `spatial_aggregate` to retrieve a result from the aggregates API.

    Parameters
    ----------
    locations : list of dicts
        List of location query specifications


    Returns
    -------
    dict
        Dict which functions as the query specification for the modal location

    """
    return {
        "query_kind": "modal_location",
        "locations": locations,
    }


def modal_location_from_dates_spec(
    *,
    start_date: str,
    end_date: str,
    aggregation_unit: str,
    method: str,
    event_types: Optional[List[str]] = None,
    subscriber_subset: Optional[Union[dict, str]] = None,
    mapping_table: Optional[str] = None,
    geom_table: Optional[str] = None,
    geom_table_join_column: Optional[str] = None,
    hours: Optional[Tuple[int, int]] = None,
) -> dict:
    """
    Return query spec for a modal location query for a date range and unit of aggregation.
    Must be passed to `spatial_aggregate` to retrieve a result from the aggregates API.

    Parameters
    ----------
    start_date : str
        ISO format date that begins the period, e.g. "2016-01-01"
    end_date : str
        ISO format date for the day _after_ the final date of the period, e.g. "2016-01-08"
    aggregation_unit : str
        Unit of aggregation, e.g. "admin3"
    method : str
        Method to use for daily locations, one of 'last' or 'most-common'
    event_types : list of {"calls", "sms", "mds", "topups"}, optional
        Optionally, include only a subset of event types (for example: ["calls", "sms"]).
        If None, include all event types in the query.
    subscriber_subset : dict or None
        Subset of subscribers to retrieve modal locations for. Must be None
        (= all subscribers), a dictionary with the specification of a
        subset query, or a string which is a valid query id.
    hours : tuple of int
        Hours of the day to include

    Returns
    -------
    dict
        Dict which functions as the query specification

    """
    dates = [
        d.strftime("%Y-%m-%d")
        for d in pd.date_range(start_date, end_date, freq="D", closed="left")
    ]
    daily_locations = [
        daily_location_spec(
            date=date,
            aggregation_unit=aggregation_unit,
            method=method,
            event_types=event_types,
            subscriber_subset=subscriber_subset,
            mapping_table=mapping_table,
            geom_table=geom_table,
            geom_table_join_column=geom_table_join_column,
            hours=hours,
        )
        for date in dates
    ]
    return modal_location_spec(locations=daily_locations)


def radius_of_gyration_spec(
    *,
    start_date: str,
    end_date: str,
    event_types: Optional[List[str]] = None,
    subscriber_subset: Optional[Union[dict, str]] = None,
    hours: Optional[Tuple[int, int]] = None,
) -> dict:
    """
    Return query spec for radius of gyration

    Parameters
    ----------
    start_date : str
        ISO format date of the first day of the count, e.g. "2016-01-01"
    end_date : str
        ISO format date of the day _after_ the final date of the count, e.g. "2016-01-08"
    event_types : list of {"calls", "sms", "mds", "topups"}, optional
        Optionally, include only a subset of event types (for example: ["calls", "sms"]).
        If None, include all event types in the query.
    subscriber_subset : dict or None, default None
        Subset of subscribers to include in event counts. Must be None
        (= all subscribers) or a dictionary with the specification of a
        subset query.
    hours : tuple of int
        Hours of the day to include

    Returns
    -------
    dict
        Dict which functions as the query specification
    """
    return {
        "query_kind": "radius_of_gyration",
        "start_date": start_date,
        "end_date": end_date,
        "event_types": event_types,
        "subscriber_subset": subscriber_subset,
        "hours": None
        if hours is None
        else dict(start_hour=hours[0], end_hour=hours[1]),
    }


def total_active_periods_spec(
    *,
    start_date: str,
    total_periods: int,
    period_unit: str = "days",
    period_length: int = 1,
    event_types: Optional[List[str]] = None,
    subscriber_subset: Optional[Union[dict, str]] = None,
    hours: Optional[Tuple[int, int]] = None,
) -> dict:
    """
    Return query spec for total active periods.

    Parameters
    ----------
    start_date : str
        ISO format date of the first day of the count, e.g. "2016-01-01"
    total_periods : int
        Total number of periods to break your time span into
    period_length : int, default 1
        Total number of days per period.
    period_unit : {'days', 'hours', 'minutes'} default 'days'
        Split this time frame into hours or days etc.
    event_types : list of {"calls", "sms", "mds", "topups"}, optional
        Optionally, include only a subset of event types (for example: ["calls", "sms"]).
        If None, include all event types in the query.
    subscriber_subset : dict or None, default None
        Subset of subscribers to include in event counts. Must be None
        (= all subscribers) or a dictionary with the specification of a
        subset query.
    hours : tuple of int
        Hours of the day to include

    Returns
    -------
    dict
        Dict which functions as the query specification
    """
    return dict(
        query_kind="total_active_periods",
        total_periods=total_periods,
        period_length=period_length,
        period_unit=period_unit,
        start_date=start_date,
        event_types=event_types,
        subscriber_subset=subscriber_subset,
        hours=None if hours is None else dict(start_hour=hours[0], end_hour=hours[1]),
    )


def unique_location_counts_spec(
    *,
    start_date: str,
    end_date: str,
    aggregation_unit: str,
    event_types: Optional[List[str]] = None,
    subscriber_subset: Optional[Union[dict, str]] = None,
    hours: Optional[Tuple[int, int]] = None,
    mapping_table: Optional[str] = None,
    geom_table: Optional[str] = None,
    geom_table_join_column: Optional[str] = None,
) -> dict:
    """
    Return query spec for unique location count

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
    subscriber_subset : dict or None, default None
        Subset of subscribers to include in event counts. Must be None
        (= all subscribers) or a dictionary with the specification of a
        subset query.
    hours : tuple of int
        Hours of the day to include

    Returns
    -------
    dict
        Dict which functions as the query specification
    """
    return {
        "query_kind": "unique_location_counts",
        "start_date": start_date,
        "end_date": end_date,
        "aggregation_unit": aggregation_unit,
        "event_types": event_types,
        "subscriber_subset": subscriber_subset,
        "mapping_table": mapping_table,
        "geom_table": geom_table,
        "geom_table_join_column": geom_table_join_column,
        "hours": None
        if hours is None
        else dict(start_hour=hours[0], end_hour=hours[1]),
    }


def topup_balance_spec(
    *,
    start_date: str,
    end_date: str,
    statistic: str,
    subscriber_subset: Optional[Union[dict, str]] = None,
    hours: Optional[Tuple[int, int]] = None,
) -> dict:
    """
    Return query spec for top-up balance.

    Parameters
    ----------
    start_date : str
        ISO format date of the first day of the count, e.g. "2016-01-01"
    end_date : str
        ISO format date of the day _after_ the final date of the count, e.g. "2016-01-08"
    statistic : {"avg", "max", "min", "median", "mode", "stddev", "variance"}
        Statistic type one of "avg", "max", "min", "median", "mode", "stddev" or "variance".
    subscriber_subset : dict or None, default None
        Subset of subscribers to include in event counts. Must be None
        (= all subscribers) or a dictionary with the specification of a
        subset query.
    hours : tuple of int
        Hours of the day to include

    Returns
    -------
    dict
        Dict which functions as the query specification
    """
    return {
        "query_kind": "topup_balance",
        "start_date": start_date,
        "end_date": end_date,
        "statistic": statistic,
        "subscriber_subset": subscriber_subset,
        "hours": None
        if hours is None
        else dict(start_hour=hours[0], end_hour=hours[1]),
    }


def subscriber_degree_spec(
    *,
    start_date: str,
    end_date: str,
    direction: str = "both",
    event_types: Optional[List[str]] = None,
    subscriber_subset: Optional[Union[dict, str]] = None,
    hours: Optional[Tuple[int, int]] = None,
) -> dict:
    """
    Return query spec for subscriber degree

    Parameters
    ----------
    start_date : str
        ISO format date of the first day of the count, e.g. "2016-01-01"
    end_date : str
        ISO format date of the day _after_ the final date of the count, e.g. "2016-01-08"
    direction : {"in", "out", "both"}, default "both"
        Optionally, include only ingoing or outbound calls/texts. Can be one of "in", "out" or "both".
    event_types : list of {"calls", "sms", "mds", "topups"}, optional
        Optionally, include only a subset of event types (for example: ["calls", "sms"]).
        If None, include all event types in the query.
    subscriber_subset : dict or None, default None
        Subset of subscribers to include in event counts. Must be None
        (= all subscribers) or a dictionary with the specification of a
        subset query.
    hours : tuple of int
        Hours of the day to include

    Returns
    -------
    dict
        Dict which functions as the query specification
    """
    return {
        "query_kind": "subscriber_degree",
        "start_date": start_date,
        "end_date": end_date,
        "direction": direction,
        "event_types": event_types,
        "subscriber_subset": subscriber_subset,
        "hours": None
        if hours is None
        else dict(start_hour=hours[0], end_hour=hours[1]),
    }


def topup_amount_spec(
    *,
    start_date: str,
    end_date: str,
    statistic: str,
    subscriber_subset: Optional[Union[dict, str]] = None,
    hours: Optional[Tuple[int, int]] = None,
) -> dict:
    """
    Return query spec for topup amount

    Parameters
    ----------
    start_date : str
        ISO format date of the first day of the count, e.g. "2016-01-01"
    end_date : str
        ISO format date of the day _after_ the final date of the count, e.g. "2016-01-08"
    statistic : {"avg", "max", "min", "median", "mode", "stddev", "variance"}
        Statistic type one of "avg", "max", "min", "median", "mode", "stddev" or "variance".
    subscriber_subset : dict or None, default None
        Subset of subscribers to include in event counts. Must be None
        (= all subscribers) or a dictionary with the specification of a
        subset query.
    hours : tuple of int
        Hours of the day to include

    Returns
    -------
    dict
        Dict which functions as the query specification
    """
    return {
        "query_kind": "topup_amount",
        "start_date": start_date,
        "end_date": end_date,
        "statistic": statistic,
        "subscriber_subset": subscriber_subset,
        "hours": None
        if hours is None
        else dict(start_hour=hours[0], end_hour=hours[1]),
    }


def event_count_spec(
    *,
    start_date: str,
    end_date: str,
    direction: str = "both",
    event_types: Optional[List[str]] = None,
    subscriber_subset: Optional[Union[dict, str]] = None,
    hours: Optional[Tuple[int, int]] = None,
) -> dict:
    """
    Return query spec for event count

    Parameters
    ----------
    start_date : str
        ISO format date of the first day of the count, e.g. "2016-01-01"
    end_date : str
        ISO format date of the day _after_ the final date of the count, e.g. "2016-01-08"
    direction : {"in", "out", "both"}, default "both"
        Optionally, include only ingoing or outbound calls/texts. Can be one of "in", "out" or "both".
    event_types : list of {"calls", "sms", "mds", "topups"}, optional
        Optionally, include only a subset of event types (for example: ["calls", "sms"]).
        If None, include all event types in the query.
    subscriber_subset : dict or None, default None
        Subset of subscribers to include in event counts. Must be None
        (= all subscribers) or a dictionary with the specification of a
        subset query.
    hours : tuple of int
        Hours of the day to include

    Returns
    -------
    dict
        Dict which functions as the query specification
    """
    return {
        "query_kind": "event_count",
        "start_date": start_date,
        "end_date": end_date,
        "direction": direction,
        "event_types": event_types,
        "subscriber_subset": subscriber_subset,
        "hours": None
        if hours is None
        else dict(start_hour=hours[0], end_hour=hours[1]),
    }


def displacement_spec(
    *,
    start_date: str,
    end_date: str,
    statistic: str,
    reference_location: Dict[str, str],
    event_types: Optional[List[str]] = None,
    subscriber_subset: Optional[Union[dict, str]] = None,
    hours: Optional[Tuple[int, int]] = None,
) -> dict:
    """
    Return query spec for displacement

    Parameters
    ----------
    start_date : str
        ISO format date of the first day of the count, e.g. "2016-01-01"
    end_date : str
        ISO format date of the day _after_ the final date of the count, e.g. "2016-01-08"
    statistic : {"avg", "max", "min", "median", "mode", "stddev", "variance"}
        Statistic type one of "avg", "max", "min", "median", "mode", "stddev" or "variance".
    reference_location : dict
        Query specification for the locations (daily or modal location) from which to
        calculate displacement.
    event_types : list of {"calls", "sms", "mds", "topups"}, optional
        Optionally, include only a subset of event types (for example: ["calls", "sms"]).
        If None, include all event types in the query.
    subscriber_subset : dict or None, default None
        Subset of subscribers to include in event counts. Must be None
        (= all subscribers) or a dictionary with the specification of a
        subset query.
    hours : tuple of int
        Hours of the day to include

    Returns
    -------
    dict
        Dict which functions as the query specification
    """
    return {
        "query_kind": "displacement",
        "start_date": start_date,
        "end_date": end_date,
        "statistic": statistic,
        "reference_location": reference_location,
        "event_types": event_types,
        "subscriber_subset": subscriber_subset,
        "hours": None
        if hours is None
        else dict(start_hour=hours[0], end_hour=hours[1]),
    }


def pareto_interactions_spec(
    *,
    start_date: str,
    end_date: str,
    proportion: float,
    event_types: Optional[List[str]] = None,
    subscriber_subset: Optional[Union[dict, str]] = None,
    hours: Optional[Tuple[int, int]] = None,
) -> dict:
    """
    Return query spec for pareto interactions

    Parameters
    ----------
    start_date : str
        ISO format date of the first day of the time interval to be considered, e.g. "2016-01-01"
    end_date : str
        ISO format date of the day _after_ the final date of the time interval to be considered, e.g. "2016-01-08"
    proportion : float
        proportion to track below
    event_types : list of {"calls", "sms", "mds", "topups"}, optional
        Optionally, include only a subset of event types (for example: ["calls", "sms"]).
        If None, include all event types in the query.
    subscriber_subset : dict or None, default None
        Subset of subscribers to include in result. Must be None
        (= all subscribers) or a dictionary with the specification of a
        subset query.
    hours : tuple of int
        Hours of the day to include

    Returns
    -------
    dict
        Dict which functions as the query specification
    """
    return {
        "query_kind": "pareto_interactions",
        "start_date": start_date,
        "end_date": end_date,
        "proportion": proportion,
        "event_types": event_types,
        "subscriber_subset": subscriber_subset,
        "hours": None
        if hours is None
        else dict(start_hour=hours[0], end_hour=hours[1]),
    }


def nocturnal_events_spec(
    *,
    start_date: str,
    end_date: str,
    hours: Tuple[int, int],
    event_types: Optional[List[str]] = None,
    subscriber_subset: Optional[Union[dict, str]] = None,
) -> dict:
    """
    Return query spec for nocturnal events

    Parameters
    ----------
    start_date : str
        ISO format date of the first day for which to count nocturnal events, e.g. "2016-01-01"
    end_date : str
        ISO format date of the day _after_ the final date for which to count nocturnal events, e.g. "2016-01-08"
    hours: tuple(int,int)
        Tuple defining beginning and end of night
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
        "query_kind": "nocturnal_events",
        "start_date": start_date,
        "end_date": end_date,
        "night_hours": dict(
            start_hour=hours[0],
            end_hour=hours[1],
        ),
        "event_types": event_types,
        "subscriber_subset": subscriber_subset,
    }


def handset_spec(
    *,
    start_date: str,
    end_date: str,
    characteristic: str = "hnd_type",
    method: str = "last",
    event_types: Optional[List[str]] = None,
    subscriber_subset: Optional[Union[dict, str]] = None,
    hours: Optional[Tuple[int, int]] = None,
) -> dict:
    """
    Return query spec for handset

    Parameters
    ----------
    start : str
        ISO format date of the first day for which to count handsets, e.g. "2016-01-01"
    stop : str
        ISO format date of the day _after_ the final date for which to count handsets, e.g. "2016-01-08"
    characteristic: {"hnd_type", "brand", "model", "software_os_name", "software_os_vendor"}, default "hnd_type"
        The required handset characteristic.
    method: {"last", "most-common"}, default "last"
        Method for choosing a handset to associate with subscriber.
    event_types : list of {"calls", "sms", "mds", "topups"}, optional
        Optionally, include only a subset of event types (for example: ["calls", "sms"]).
        If None, include all event types in the query.
    subscriber_subset : dict or None, default None
        Subset of subscribers to include in event counts. Must be None
        (= all subscribers) or a dictionary with the specification of a
        subset query.
    hours : tuple of int
        Hours of the day to include

    Returns
    -------
    dict
        Dict which functions as the query specification
    """
    return {
        "query_kind": "handset",
        "start_date": start_date,
        "end_date": end_date,
        "characteristic": characteristic,
        "method": method,
        "event_types": event_types,
        "subscriber_subset": subscriber_subset,
        "hours": None
        if hours is None
        else dict(start_hour=hours[0], end_hour=hours[1]),
    }


def random_sample_spec(
    *,
    query: Dict[str, Union[str, dict]],
    seed: float,
    sampling_method: str = "random_ids",
    size: Union[int, None] = None,
    fraction: Union[float, None] = None,
    estimate_count: bool = True,
) -> dict:
    """
    Return spec for a random sample from a query result.

    Parameters
    ----------
    query : dict
        Specification of the query to be sampled.
    sampling_method : {'system', 'bernoulli', 'random_ids'}, default 'random_ids'
        Specifies the method used to select the random sample.
        'system': performs block-level sampling by randomly sampling each
            physical storage page for the underlying relation. This
            sampling method is not guaranteed to generate a sample of the
            specified size, but an approximation. This method may not
            produce a sample at all, so it might be worth running it again
            if it returns an empty dataframe.
        'bernoulli': samples directly on each row of the underlying
            relation. This sampling method is slower and is not guaranteed to
            generate a sample of the specified size, but an approximation
        'random_ids': samples rows by randomly sampling the row number.
    size : int, optional
        The number of rows to draw.
        Exactly one of the 'size' or 'fraction' arguments must be provided.
    fraction : float, optional
        Fraction of rows to draw.
        Exactly one of the 'size' or 'fraction' arguments must be provided.
    estimate_count : bool, default True
        Whether to estimate the number of rows in the table using
        information contained in the `pg_class` or whether to perform an
        actual count in the number of rows.
    seed : float
        A seed for repeatable random samples.
        If using random_ids method, seed must be between -/+1.

    Returns
    -------
    dict
        Dict which functions as the query specification.
    """
    sampled_query = dict(query)
    sampling = dict(
        seed=seed,
        sampling_method=sampling_method,
        size=size,
        fraction=fraction,
        estimate_count=estimate_count,
    )
    sampled_query["sampling"] = sampling
    return sampled_query


def majority_location_spec(
    *, subscriber_location_weights: Dict, include_unlocatable=False
) -> dict:
    """
    A class for producing a list of subscribers along with the location (derived from `spatial_unit')
    that they visited more than half the time.

    Parameters
    ----------
    subscriber_location_weights: dict
        A `location_visits_spec`  query specification
    include_unlocatable: bool default False
        If `True`, returns every unique subscriber in the `subscriber_location_weights` query, with
        the location column as `NULL` if no majority is reached.
        If `False`, returns only subscribers that have achieved a majority location

    Returns
    -------
    dict
        A dictionary of the query specification


    """
    return {
        "query_kind": "majority_location",
        "subscriber_location_weights": subscriber_location_weights,
        "include_unlocatable": include_unlocatable,
    }


def location_visits_spec(*, locations: List) -> dict:
    """
    Class that defines lists of unique Dailylocations for each subscriber.
    Each location is accompanied by the count of times it was a daily_location.

    Parameters
    ----------
    locations : List
        A list of either daily_location or modal_location query specs

    Returns
    -------
    dict
        A dictionary specifying a location_visits query

    """
    return {"query_kind": "location_visits", "locations": locations}


def coalesced_location_spec(
    *,
    preferred_location,
    fallback_location,
    subscriber_location_weights,
    weight_threshold,
) -> dict:
    return {
        "query_kind": "coalesced_location",
        "preferred_location": preferred_location,
        "fallback_location": fallback_location,
        "subscriber_location_weights": subscriber_location_weights,
        "weight_threshold": weight_threshold,
    }


def mobility_classification_spec(
    *, locations: List[Dict[str, Any]], stay_length_threshold: int
) -> dict:
    """
    Based on subscribers' reference locations in a sequence of reference
    periods, classify each subscriber as having one of the following mobility
    types (the assigned label corresponds to the first of these criteria that
    is true for a given subscriber):

    - 'unlocated': Subscriber has a NULL location in the most recent period
    - 'irregular': Subscriber is not active in at least one of the reference
      periods
    - 'not_always_locatable': Subscriber has a NULL location in at least one of
      the reference periods
    - 'mobile': Subscriber spent fewer than 'stay_length_threshold' consecutive
      periods at any single location
    - 'stable': Subscriber spent at least 'stay_length_threshold' consecutive
      periods at the same location
    Only subscribers appearing in the result of the reference location query
    for the most recent period are included in the result of this query (i.e.
    subscribers absent from the query result can be assumed to fall into a
    sixth category: "not active in the most recent period").

    Parameters
    ----------
    locations : list of reference location query specs
        List of reference location queries, each returning a single location
        per subscriber (or NULL location for subscribers that are active but
        unlocatable). The list is assumed to be sorted into ascending
        chronological order.
    stay_length_threshold : int
        Minimum number of consecutive periods over which a subscriber's
        location must remain the same for that subscriber to be classified as
        'stable'.

    Returns
    -------
    dict
        A dictionary specifying a mobility_classification query
    """
    return {
        "query_kind": "mobility_classification",
        "locations": locations,
        "stay_length_threshold": stay_length_threshold,
    }
