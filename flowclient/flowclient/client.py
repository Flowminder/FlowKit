# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import logging
import warnings
import re

import jwt
import pandas as pd
import requests
import time
from requests import ConnectionError
from typing import Tuple, Union, Dict, List, Optional

logger = logging.getLogger(__name__)


class FlowclientConnectionError(Exception):
    """
    Custom exception to indicate an error when connecting to a FlowKit API.
    """


class Connection:
    """
    A connection to a FlowKit API server.

    Attributes
    ----------
    url : str
        URL of the API server
    token : str
        JSON Web Token for this API server
    api_version : int
        Version of the API to connect to
    user : str
        Username of token

    Parameters
    ----------
    url : str
        URL of the API server, e.g. "https://localhost:9090"
    token : str
        JSON Web Token for this API server
    api_version : int, default 0
        Version of the API to connect to
    ssl_certificate: str or None
        Provide a path to an ssl certificate to use, or None to use
        default root certificates.
    """

    url: str
    token: str
    user: str
    api_version: int

    def __init__(
        self,
        *,
        url: str,
        token: str,
        api_version: int = 0,
        ssl_certificate: Union[str, None] = None,
    ) -> None:
        if not url.lower().startswith("https://"):
            warnings.warn(
                "Communications with this server are NOT SECURE.", stacklevel=2
            )
        self.url = url
        self.token = token
        self.api_version = api_version
        try:
            self.user = jwt.decode(token, verify=False)["identity"]
        except jwt.DecodeError:
            raise FlowclientConnectionError(f"Unable to decode token: '{token}'")
        except KeyError:
            raise FlowclientConnectionError(f"Token does not contain user identity.")
        self.session = requests.Session()
        if ssl_certificate is not None:
            self.session.verify = ssl_certificate
        self.session.headers["Authorization"] = f"Bearer {self.token}"

    def get_url(
        self, *, route: str, data: Union[None, dict] = None
    ) -> requests.Response:
        """
        Attempt to get something from the API, and return the raw
        response object if an error response wasn't received.
        If an error response was received, raises an error.

        Parameters
        ----------
        route : str
            Path relative to API host to get

        data : dict, optional
            JSON data to send in the request body (optional)

        Returns
        -------
        requests.Response

        """
        logger.debug(f"Getting {self.url}/api/{self.api_version}/{route}")
        try:
            response = self.session.get(
                f"{self.url}/api/{self.api_version}/{route}",
                allow_redirects=False,
                json=data,
            )
        except ConnectionError as e:
            error_msg = f"Unable to connect to FlowKit API at {self.url}: {e}"
            logger.info(error_msg)
            raise FlowclientConnectionError(error_msg)
        if response.status_code in {202, 200, 303}:
            return response
        elif response.status_code == 404:
            raise FileNotFoundError(
                f"{self.url}/api/{self.api_version}/{route} not found."
            )
        elif response.status_code in {401, 403}:
            try:
                error = response.json()["msg"]
            except (ValueError, KeyError):
                error = "Unknown access denied error"
            raise FlowclientConnectionError(error)
        else:
            try:
                error = response.json()["msg"]
            except (ValueError, KeyError):
                error = "Unknown error"
            try:
                status = response.json()["status"]
            except (ValueError, KeyError):
                status = "Unknown status"
            raise FlowclientConnectionError(
                f"Something went wrong: {error}. API returned with status code: {response.status_code} and status '{status}'"
            )

    def post_json(self, *, route: str, data: dict) -> requests.Response:
        """
        Attempt to post json to the API, and return the raw
        response object if an error response wasn't received.
        If an error response was received, raises an error.

        Parameters
        ----------
        route : str
            Path relative to API host to post_json to
        data: dict
            Dictionary of json-encodeable data to post_json

        Returns
        -------
        requests.Response

        """
        logger.debug(f"Posting {data} to {self.url}/api/{self.api_version}/{route}")
        try:
            response = self.session.post(
                f"{self.url}/api/{self.api_version}/{route}", json=data
            )
        except ConnectionError as e:
            error_msg = f"Unable to connect to FlowKit API at {self.url}: {e}"
            logger.info(error_msg)
            raise FlowclientConnectionError(error_msg)
        if response.status_code == 202:
            return response
        elif response.status_code == 404:
            raise FileNotFoundError(
                f"{self.url}/api/{self.api_version}/{route} not found."
            )
        elif response.status_code in {401, 403}:
            try:
                error_msg = response.json()["msg"]
            except ValueError:
                error_msg = "Unknown access denied error"
            raise FlowclientConnectionError(error_msg)
        else:
            try:
                error_msg = response.json()["msg"]
                try:
                    returned_payload = response.json()["payload"]
                    payload_info = (
                        "" if not returned_payload else f" Payload: {returned_payload}"
                    )
                except KeyError:
                    payload_info = ""
            except ValueError:
                # Happens if the response body does not contain valid JSON
                # (see http://docs.python-requests.org/en/master/api/#requests.Response.json)
                error_msg = f"the response did not contain valid JSON"
                payload_info = ""
            raise FlowclientConnectionError(
                f"Something went wrong. API returned with status code {response.status_code}. Error message: '{error_msg}'.{payload_info}"
            )

    def __repr__(self) -> str:
        return f"{self.user}@{self.url} v{self.api_version}"


def connect(
    *,
    url: str,
    token: str,
    api_version: int = 0,
    ssl_certificate: Union[str, None] = None,
) -> Connection:
    """
    Connect to a FlowKit API server and return the resulting Connection object.

    Parameters
    ----------
    url : str
        URL of the API server, e.g. "https://localhost:9090"
    token : str
        JSON Web Token for this API server
    api_version : int, default 0
        Version of the API to connect to
    ssl_certificate: str or None
        Provide a path to an ssl certificate to use, or None to use
        default root certificates.

    Returns
    -------
    Connection
    """
    return Connection(
        url=url, token=token, api_version=api_version, ssl_certificate=ssl_certificate
    )


def query_is_ready(
    *, connection: Connection, query_id: str
) -> Tuple[bool, requests.Response]:
    """
    Check if a query id has results available.

    Parameters
    ----------
    connection : Connection
        API connection  to use
    query_id : str
        Identifier of the query to retrieve

    Returns
    -------
    Tuple[bool, requests.Response]
        True if the query result is available

    """
    logger.info(
        f"Polling server on {connection.url}/api/{connection.api_version}/poll/{query_id}"
    )
    reply = connection.get_url(route=f"poll/{query_id}")

    if reply.status_code == 303:
        logger.info(
            f"{connection.url}/api/{connection.api_version}/poll/{query_id} ready."
        )
        return True, reply  # Query is ready, so exit the loop
    elif reply.status_code == 202:
        logger.info(
            "{eligible} parts to run, {queued} in queue and {running} running.".format(
                **reply.json()["progress"]
            )
        )
        return False, reply
    else:
        raise FlowclientConnectionError(
            f"Something went wrong: {reply}. API returned with status code: {reply.status_code}"
        )


def get_status(*, connection: Connection, query_id: str) -> str:
    """
    Check the status of a query.

    Parameters
    ----------
    connection : Connection
        API connection  to use
    query_id : str
        Identifier of the query to retrieve

    Returns
    -------
    str
        "Finished" or "Running"

    """
    ready, reply = query_is_ready(connection=connection, query_id=query_id)
    if ready:
        return "Finished"
    else:
        try:
            return reply.json()["status"]
        except (KeyError, TypeError):
            raise FlowclientConnectionError(f"No status reported.")


def wait_for_query_to_be_ready(
    *, connection: Connection, query_id: str, poll_interval: int = 1
) -> requests.Response:
    """
    Wait until a query id has finished running, and if it finished successfully
    return the reply from flowapi.

    Parameters
    ----------
    connection : Connection
        API connection  to use
    query_id : str
        Identifier of the query to retrieve
    poll_interval : int
        Number of seconds to wait between checks for the query being ready

    Returns
    -------
    requests.Response
        Response object containing the reply to flowapi

    Raises
    ------
    FlowclientConnectionError
        If the query has finished running unsuccessfully
    """
    query_ready, reply = query_is_ready(
        connection=connection, query_id=query_id
    )  # Poll the server
    while not query_ready:
        logger.info("Waiting before polling again.")
        time.sleep(
            poll_interval
        )  # Wait a second, then check if the query is ready again
        query_ready, reply = query_is_ready(
            connection=connection, query_id=query_id
        )  # Poll the server
    return reply


def get_result_location_from_id_when_ready(
    *, connection: Connection, query_id: str, poll_interval: int = 1
) -> str:
    """
    Return, once ready, the location at which results of a query will be obtainable.

    Parameters
    ----------
    connection : Connection
        API connection  to use
    query_id : str
        Identifier of the query to retrieve
    poll_interval : int
        Number of seconds to wait between checks for the query being ready

    Returns
    -------
    str
        Endpoint to retrieve results from

    """
    reply = wait_for_query_to_be_ready(
        connection=connection, query_id=query_id, poll_interval=poll_interval
    )

    result_location = reply.headers[
        "Location"
    ]  # Need to strip off the /api/<api_version>/
    return re.sub(
        "^/api/[0-9]+/", "", result_location
    )  # strip off the /api/<api_version>/


def get_json_dataframe(*, connection: Connection, location: str) -> pd.DataFrame:
    """
    Get a dataframe from a json source.

    Parameters
    ----------
    connection : Connection
        API connection  to use
    location : str
        API enpoint to retrieve json from

    Returns
    -------
    pandas.DataFrame
        Dataframe containing the result

    """

    response = connection.get_url(route=location)
    if response.status_code != 200:
        try:
            msg = response.json()["msg"]
            more_info = f" Reason: {msg}"
        except KeyError:
            more_info = ""
        raise FlowclientConnectionError(
            f"Could not get result. API returned with status code: {response.status_code}.{more_info}"
        )
    result = response.json()
    logger.info(f"Got {connection.url}/api/{connection.api_version}/{location}")
    return pd.DataFrame.from_records(result["query_result"])


def get_geojson_result_by_query_id(
    *, connection: Connection, query_id: str, poll_interval: int = 1
) -> dict:
    """
    Get a query by id, and return it as a geojson dict

    Parameters
    ----------
    connection : Connection
        API connection  to use
    query_id : str
        Identifier of the query to retrieve
    poll_interval : int
        Number of seconds to wait between checks for the query being ready

    Returns
    -------
    dict
        geojson

    """
    result_endpoint = get_result_location_from_id_when_ready(
        connection=connection, query_id=query_id, poll_interval=poll_interval
    )
    response = connection.get_url(route=f"{result_endpoint}.geojson")
    if response.status_code != 200:
        try:
            msg = response.json()["msg"]
            more_info = f" Reason: {msg}"
        except KeyError:
            more_info = ""
        raise FlowclientConnectionError(
            f"Could not get result. API returned with status code: {response.status_code}.{more_info}"
        )
    return response.json()


def get_result_by_query_id(
    *, connection: Connection, query_id: str, poll_interval: int = 1
) -> pd.DataFrame:
    """
    Get a query by id, and return it as a dataframe

    Parameters
    ----------
    connection : Connection
        API connection  to use
    query_id : str
        Identifier of the query to retrieve
    poll_interval : int
        Number of seconds to wait between checks for the query being ready

    Returns
    -------
    pandas.DataFrame
        Dataframe containing the result

    """
    result_endpoint = get_result_location_from_id_when_ready(
        connection=connection, query_id=query_id, poll_interval=poll_interval
    )
    return get_json_dataframe(connection=connection, location=result_endpoint)


def get_geojson_result(*, connection: Connection, query: dict) -> dict:
    """
    Run and retrieve a query of a specified kind with parameters.

    Parameters
    ----------
    connection : Connection
        API connection to use
    query : dict
        A query specification to run, e.g. `{'kind':'daily_location', 'params':{'date':'2016-01-01'}}`

    Returns
    -------
    dict
       Geojson

    """
    return get_geojson_result_by_query_id(
        connection=connection, query_id=run_query(connection=connection, query=query)
    )


def get_result(*, connection: Connection, query: dict) -> pd.DataFrame:
    """
    Run and retrieve a query of a specified kind with parameters.

    Parameters
    ----------
    connection : Connection
        API connection to use
    query : dict
        A query specification to run, e.g. `{'kind':'daily_location', 'params':{'date':'2016-01-01'}}`

    Returns
    -------
    pd.DataFrame
       Pandas dataframe containing the results

    """
    return get_result_by_query_id(
        connection=connection, query_id=run_query(connection=connection, query=query)
    )


def get_geography(*, connection: Connection, aggregation_unit: str) -> dict:
    """
    Get geography data from the database.

    Parameters
    ----------
    connection : Connection
        API connection to use
    aggregation_unit : str
        aggregation unit, e.g. 'admin3'

    Returns
    -------
    dict
        geography data as a GeoJSON FeatureCollection

    """
    logger.info(
        f"Getting {connection.url}/api/{connection.api_version}/geography/{aggregation_unit}"
    )
    response = connection.get_url(route=f"geography/{aggregation_unit}")
    if response.status_code != 200:
        try:
            msg = response.json()["msg"]
            more_info = f" Reason: {msg}"
        except KeyError:
            more_info = ""
        raise FlowclientConnectionError(
            f"Could not get result. API returned with status code: {response.status_code}.{more_info}"
        )
    result = response.json()
    logger.info(
        f"Got {connection.url}/api/{connection.api_version}/geography/{aggregation_unit}"
    )
    return result


def get_available_dates(
    *, connection: Connection, event_types: Union[None, List[str]] = None
) -> dict:
    """
    Get available dates for different event types from the database.

    Parameters
    ----------
    connection : Connection
        API connection to use
    event_types : list of str, optional
        The event types for which to return available dates (for example: ["calls", "sms"]).
        If None, return available dates for all available event types.

    Returns
    -------
    dict
        Available dates in the format {event_type: [list of dates]}

    """
    logger.info(
        f"Getting {connection.url}/api/{connection.api_version}/available_dates"
    )
    response = connection.get_url(route=f"available_dates")
    if response.status_code != 200:
        try:
            msg = response.json()["msg"]
            more_info = f" Reason: {msg}"
        except KeyError:
            more_info = ""
        raise FlowclientConnectionError(
            f"Could not get available dates. API returned with status code: {response.status_code}.{more_info}"
        )
    result = response.json()["available_dates"]
    logger.info(f"Got {connection.url}/api/{connection.api_version}/available_dates")
    if event_types is None:
        return result
    else:
        return {k: v for k, v in result.items() if k in event_types}


def run_query(*, connection: Connection, query: dict) -> str:
    """
    Run a query of a specified kind with parameters and get the identifier for it.

    Parameters
    ----------
    connection : Connection
        API connection to use
    query : dict
        Query to run

    Returns
    -------
    str
        Identifier of the query
    """
    logger.info(
        f"Requesting run of {query} at {connection.url}/api/{connection.api_version}"
    )
    r = connection.post_json(route="run", data=query)
    if r.status_code == 202:
        query_id = r.headers["Location"].split("/").pop()
        logger.info(
            f"Accepted {query} at {connection.url}/api/{connection.api_version} with id {query_id}"
        )
        return query_id
    else:
        try:
            error = r.json()["msg"]
        except (ValueError, KeyError):
            error = "Unknown error."
        raise FlowclientConnectionError(
            f"Error running the query: {error}. Status code: {r.status_code}."
        )


def location_event_counts(
    *,
    start_date: str,
    end_date: str,
    aggregation_unit: str,
    count_interval: str,
    direction: str = "both",
    event_types: Union[None, List[str]] = None,
    subscriber_subset: Union[dict, None] = None,
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
    event_types : None or list of {"calls", "sms", "mds"}, default None
        Optionally, include only a subset of events. Can be one of "calls", "sms" or "mds"
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
    }


def daily_location(
    *,
    date: str,
    aggregation_unit: str,
    method: str,
    subscriber_subset: Union[dict, None] = None,
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
    }


def meaningful_locations_aggregate(
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
    subscriber_subset: Union[dict, None] = None,
) -> dict:
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
    subscriber_subset : dict or None
        Subset of subscribers to retrieve modal locations for. Must be None
        (= all subscribers) or a dictionary with the specification of a
        subset query.

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
        "subscriber_subset": subscriber_subset,
    }


def meaningful_locations_between_label_od_matrix(
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
    subscriber_subset: Union[dict, None] = None,
) -> dict:
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
    subscriber_subset : dict or None
        Subset of subscribers to retrieve modal locations for. Must be None
        (= all subscribers) or a dictionary with the specification of a
        subset query.

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
        "subscriber_subset": subscriber_subset,
    }


def meaningful_locations_between_dates_od_matrix(
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
    subscriber_subset: Union[dict, None] = None,
) -> dict:
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
    subscriber_subset : dict or None
        Subset of subscribers to retrieve modal locations for. Must be None
        (= all subscribers) or a dictionary with the specification of a
        subset query.

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
        "subscriber_subset": subscriber_subset,
    }


def modal_location(*, locations: List[Dict[str, Union[str, Dict[str, str]]]]) -> dict:
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


def modal_location_from_dates(
    *,
    start_date: str,
    end_date: str,
    aggregation_unit: str,
    method: str,
    subscriber_subset: Union[dict, None] = None,
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
        daily_location(
            date=date,
            aggregation_unit=aggregation_unit,
            method=method,
            subscriber_subset=subscriber_subset,
        )
        for date in dates
    ]
    return modal_location(locations=daily_locations)


def flows(
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


def unique_subscriber_counts(
    *, start_date: str, end_date: str, aggregation_unit: str
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
    }


def location_introversion(
    *, start_date: str, end_date: str, aggregation_unit: str, direction: str = "both"
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
    }


def total_network_objects(
    *, start_date: str, end_date: str, aggregation_unit: str, total_by: str = "day"
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
    }


def radius_of_gyration(
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


def spatial_aggregate(*, locations: Dict[str, Union[str, Dict[str, str]]]) -> dict:
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


def joined_spatial_aggregate(
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


def aggregate_network_objects(
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


def unique_location_counts(
    *,
    start_date: str,
    end_date: str,
    aggregation_unit: str,
    subscriber_subset: Union[dict, None] = None,
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
    }


def topup_balance(
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


def subscriber_degree(
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


def topup_amount(
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


def event_count(
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


def displacement(
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


def pareto_interactions(
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


def nocturnal_events(
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


def handset(
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


def random_sample(
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
