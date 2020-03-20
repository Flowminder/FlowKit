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
        self.api_version = api_version
        self.session = requests.Session()
        if ssl_certificate is not None:
            self.session.verify = ssl_certificate
        self.update_token(token=token)

    def update_token(self, token: str) -> None:
        """
        Replace this connection's API token with a new one.

        Parameters
        ----------
        token : str
            JSON Web Token for this API server
        """
        try:
            self.user = jwt.decode(token, verify=False)["identity"]
        except jwt.DecodeError:
            raise FlowclientConnectionError(f"Unable to decode token: '{token}'")
        except KeyError:
            raise FlowclientConnectionError(f"Token does not contain user identity.")
        self.token = token
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

    Raises
    ------
    FlowclientConnectionError
        if query has errored
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
        API connection to use
    query_id : str
        Identifier of the query to retrieve

    Returns
    -------
    str
        Query status

    Raises
    ------
    FlowclientConnectionError
        if response does not contain the query status
    """
    try:
        ready, reply = query_is_ready(connection=connection, query_id=query_id)
    except FileNotFoundError:
        # Can't distinguish 'known', 'cancelled', 'resetting' and 'awol' from the error,
        # so return generic 'not_running' status.
        return "not_running"

    if ready:
        return "completed"
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


def get_geojson_result(*, connection: Connection, query_spec: dict) -> dict:
    """
    Run and retrieve a query of a specified kind with parameters.

    Parameters
    ----------
    connection : Connection
        API connection to use
    query_spec : dict
        A query specification to run, e.g. `{'kind':'daily_location', 'params':{'date':'2016-01-01'}}`

    Returns
    -------
    dict
       Geojson

    """
    return get_geojson_result_by_query_id(
        connection=connection,
        query_id=run_query(connection=connection, query_spec=query_spec),
    )


def get_result(*, connection: Connection, query_spec: dict) -> pd.DataFrame:
    """
    Run and retrieve a query of a specified kind with parameters.

    Parameters
    ----------
    connection : Connection
        API connection to use
    query_spec : dict
        A query specification to run, e.g. `{'kind':'daily_location', 'date':'2016-01-01'}`

    Returns
    -------
    pd.DataFrame
       Pandas dataframe containing the results

    """
    return get_result_by_query_id(
        connection=connection,
        query_id=run_query(connection=connection, query_spec=query_spec),
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


def run_query(*, connection: Connection, query_spec: dict) -> str:
    """
    Run a query of a specified kind with parameters and get the identifier for it.

    Parameters
    ----------
    connection : Connection
        API connection to use
    query_spec : dict
        Query specification to run

    Returns
    -------
    str
        Identifier of the query
    """
    logger.info(
        f"Requesting run of {query_spec} at {connection.url}/api/{connection.api_version}"
    )
    r = connection.post_json(route="run", data=query_spec)
    if r.status_code == 202:
        query_id = r.headers["Location"].split("/").pop()
        logger.info(
            f"Accepted {query_spec} at {connection.url}/api/{connection.api_version} with id {query_id}"
        )
        return query_id
    else:
        try:
            error = r.json()["msg"]
        except (ValueError, KeyError):
            error = "Unknown error"
        raise FlowclientConnectionError(
            f"Error running the query: {error}. Status code: {r.status_code}."
        )


def daily_location_spec(
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
