# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from typing import Union, List, Dict, Optional, Tuple

import pandas as pd


def unique_locations_spec(
    *,
    start_date: str,
    end_date: str,
    aggregation_unit: str,
    subscriber_subset: Union[dict, None] = None,
    mapping_table: Optional[str] = None,
    geom_table: Optional[str] = None,
    geom_table_join_column: Optional[str] = None,
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
    subscriber_subset : dict or None
        Subset of subscribers to retrieve daily locations for. Must be None
        (= all subscribers) or a dictionary with the specification of a
        subset query.

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
    subscriber_subset: Union[dict, None] = None,
    mapping_table: Optional[str] = None,
    geom_table: Optional[str] = None,
    geom_table_join_column: Optional[str] = None,
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
    subscriber_subset : dict or None
        Subset of subscribers to retrieve daily locations for. Must be None
        (= all subscribers) or a dictionary with the specification of a
        subset query.

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
        "subscriber_subset": subscriber_subset,
        "mapping_table": mapping_table,
        "geom_table": geom_table,
        "geom_table_join_column": geom_table_join_column,
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
    subscriber_subset: Union[dict, None] = None,
    mapping_table: Optional[str] = None,
    geom_table: Optional[str] = None,
    geom_table_join_column: Optional[str] = None,
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
    subscriber_subset : dict or None
        Subset of subscribers to retrieve modal locations for. Must be None
        (= all subscribers) or a dictionary with the specification of a
        subset query.

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
            subscriber_subset=subscriber_subset,
            mapping_table=mapping_table,
            geom_table=geom_table,
            geom_table_join_column=geom_table_join_column,
        )
        for date in dates
    ]
    return modal_location_spec(locations=daily_locations)


def radius_of_gyration_spec(
    *, start_date: str, end_date: str, subscriber_subset: Union[dict, None] = None
) -> dict:
    """
    Return query spec for radius of gyration

    Parameters
    ----------
    start_date : str
        ISO format date of the first day of the count, e.g. "2016-01-01"
    end_date : str
        ISO format date of the day _after_ the final date of the count, e.g. "2016-01-08"
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
        "query_kind": "radius_of_gyration",
        "start_date": start_date,
        "end_date": end_date,
        "subscriber_subset": subscriber_subset,
    }


def unique_location_counts_spec(
    *,
    start_date: str,
    end_date: str,
    aggregation_unit: str,
    subscriber_subset: Union[dict, None] = None,
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
        "query_kind": "unique_location_counts",
        "start_date": start_date,
        "end_date": end_date,
        "aggregation_unit": aggregation_unit,
        "subscriber_subset": subscriber_subset,
        "mapping_table": mapping_table,
        "geom_table": geom_table,
        "geom_table_join_column": geom_table_join_column,
    }


def topup_balance_spec(
    *,
    start_date: str,
    end_date: str,
    statistic: str,
    subscriber_subset: Union[dict, None] = None,
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
    }


def subscriber_degree_spec(
    *,
    start: str,
    stop: str,
    direction: str = "both",
    subscriber_subset: Union[dict, None] = None,
) -> dict:
    """
    Return query spec for subscriber degree

    Parameters
    ----------
    start : str
        ISO format date of the first day of the count, e.g. "2016-01-01"
    stop : str
        ISO format date of the day _after_ the final date of the count, e.g. "2016-01-08"
    direction : {"in", "out", "both"}, default "both"
        Optionally, include only ingoing or outbound calls/texts. Can be one of "in", "out" or "both".
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
        "query_kind": "subscriber_degree",
        "start": start,
        "stop": stop,
        "direction": direction,
        "subscriber_subset": subscriber_subset,
    }


def topup_amount_spec(
    *,
    start: str,
    stop: str,
    statistic: str,
    subscriber_subset: Union[dict, None] = None,
) -> dict:
    """
    Return query spec for topup amount

    Parameters
    ----------
    start : str
        ISO format date of the first day of the count, e.g. "2016-01-01"
    stop : str
        ISO format date of the day _after_ the final date of the count, e.g. "2016-01-08"
    statistic : {"avg", "max", "min", "median", "mode", "stddev", "variance"}
        Statistic type one of "avg", "max", "min", "median", "mode", "stddev" or "variance".
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
        "query_kind": "topup_amount",
        "start": start,
        "stop": stop,
        "statistic": statistic,
        "subscriber_subset": subscriber_subset,
    }


def event_count_spec(
    *,
    start: str,
    stop: str,
    direction: str = "both",
    event_types: Optional[List[str]] = None,
    subscriber_subset: Union[dict, None] = None,
) -> dict:
    """
    Return query spec for event count

    Parameters
    ----------
    start : str
        ISO format date of the first day of the count, e.g. "2016-01-01"
    stop : str
        ISO format date of the day _after_ the final date of the count, e.g. "2016-01-08"
    direction : {"in", "out", "both"}, default "both"
        Optionally, include only ingoing or outbound calls/texts. Can be one of "in", "out" or "both".
    event_types : list of str, optional
        The event types to include in the count (for example: ["calls", "sms"]).
        If None, include all event types in the count.
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
        "query_kind": "event_count",
        "start": start,
        "stop": stop,
        "direction": direction,
        "event_types": event_types,
        "subscriber_subset": subscriber_subset,
    }


def displacement_spec(
    *,
    start: str,
    stop: str,
    statistic: str,
    reference_location: Dict[str, str],
    subscriber_subset: Union[dict, None] = None,
) -> dict:
    """
    Return query spec for displacement

    Parameters
    ----------
    start : str
        ISO format date of the first day of the count, e.g. "2016-01-01"
    stop : str
        ISO format date of the day _after_ the final date of the count, e.g. "2016-01-08"
    statistic : {"avg", "max", "min", "median", "mode", "stddev", "variance"}
        Statistic type one of "avg", "max", "min", "median", "mode", "stddev" or "variance".
    reference_location:

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
        "query_kind": "displacement",
        "start": start,
        "stop": stop,
        "statistic": statistic,
        "reference_location": reference_location,
        "subscriber_subset": subscriber_subset,
    }


def pareto_interactions_spec(
    *,
    start: str,
    stop: str,
    proportion: float,
    subscriber_subset: Union[dict, None] = None,
) -> dict:
    """
    Return query spec for pareto interactions

    Parameters
    ----------
    start : str
        ISO format date of the first day of the time interval to be considered, e.g. "2016-01-01"
    stop : str
        ISO format date of the day _after_ the final date of the time interval to be considered, e.g. "2016-01-08"
    proportion : float
        proportion to track below
    subscriber_subset : dict or None, default None
        Subset of subscribers to include in result. Must be None
        (= all subscribers) or a dictionary with the specification of a
        subset query.

    Returns
    -------
    dict
        Dict which functions as the query specification
    """
    return {
        "query_kind": "pareto_interactions",
        "start": start,
        "stop": stop,
        "proportion": proportion,
        "subscriber_subset": subscriber_subset,
    }


def nocturnal_events_spec(
    *,
    start: str,
    stop: str,
    hours: Tuple[int, int],
    subscriber_subset: Union[dict, None] = None,
) -> dict:
    """
    Return query spec for nocturnal events

    Parameters
    ----------
    start : str
        ISO format date of the first day for which to count nocturnal events, e.g. "2016-01-01"
    stop : str
        ISO format date of the day _after_ the final date for which to count nocturnal events, e.g. "2016-01-08"
    hours: tuple(int,int)
        Tuple defining beginning and end of night

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
        "start": start,
        "stop": stop,
        "night_start_hour": hours[0],
        "night_end_hour": hours[1],
        "subscriber_subset": subscriber_subset,
    }


def handset_spec(
    *,
    start_date: str,
    end_date: str,
    characteristic: str = "hnd_type",
    method: str = "last",
    subscriber_subset: Union[dict, None] = None,
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
        "query_kind": "handset",
        "start_date": start_date,
        "end_date": end_date,
        "characteristic": characteristic,
        "method": method,
        "subscriber_subset": subscriber_subset,
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
