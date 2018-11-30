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
from typing import Tuple, Union, Dict

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

    def get_url(self, route: str) -> requests.Response:
        """
        Attempt to get something from the API, and return the raw
        response object if an error response wasn't received.
        If an error response was received, raises an error.

        Parameters
        ----------
        route : str
            Path relative to API host to get

        Returns
        -------
        requests.Response

        """
        logger.debug(f"Getting {self.url}/api/{self.api_version}/{route}")
        try:
            response = self.session.get(
                f"{self.url}/api/{self.api_version}/{route}", allow_redirects=False
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
        elif response.status_code == 401:
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
            raise FlowclientConnectionError(
                f"Something went wrong: {error}. API returned with status code: {response.status_code}"
            )

    def post_json(self, route: str, data: dict) -> requests.Response:
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
        elif response.status_code == 401:
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
            raise FlowclientConnectionError(
                f"Something went wrong: {error}. API returned with status code: {response.status_code}"
            )

    def __repr__(self) -> str:
        return f"{self.user}@{self.url} v{self.api_version}"


def connect(
    url: str, token: str, api_version: int = 0, ssl_certificate: Union[str, None] = None
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

    Returns
    -------
    Connection
    """
    return Connection(url, token, api_version, ssl_certificate)


def query_is_ready(
    connection: Connection, query_id: str
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
    reply = connection.get_url(f"poll/{query_id}")

    if reply.status_code == 303:
        logger.info(
            f"{connection.url}/api/{connection.api_version}/poll/{query_id} ready."
        )
        return True, reply  # Query is ready, so exit the loop
    elif reply.status_code == 202:
        return False, reply
    else:
        raise FlowclientConnectionError(
            f"Something went wrong: {reply}. API returned with status code: {reply.status_code}"
        )


def get_status(connection: Connection, query_id: str) -> str:
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
    ready, status_code = query_is_ready(connection, query_id)
    if ready:
        return "Finished"
    else:
        return "Running"


def get_result_by_query_id(connection: Connection, query_id: str) -> pd.DataFrame:
    """
    Get a query by id, and return it as a dataframe

    Parameters
    ----------
    connection : Connection
        API connection  to use
    query_id : str
        Identifier of the query to retrieve

    Returns
    -------
    pandas.DataFrame
        Dataframe containing the result

    """
    query_ready, reply = query_is_ready(connection, query_id)  # Poll the server
    while not query_ready:
        logger.info("Waiting before polling again.")
        time.sleep(1)  # Wait a second, then check if the query is ready again
        query_ready, reply = query_is_ready(connection, query_id)  # Poll the server

    logger.info(f"Getting {connection.url}/api/{connection.api_version}/get/{query_id}")
    result_location = reply.headers[
        "Location"
    ]  # Need to strip off the /api/<api_version>/
    result_location = re.sub(
        "^/api/[0-9]+/", "", result_location
    )  # strip off the /api/<api_version>/
    response = connection.get_url(result_location)
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
    logger.info(f"Got {connection.url}/api/{connection.api_version}/{query_id}")
    return pd.DataFrame.from_records(result["query_result"])


def get_result(connection: Connection, query: dict) -> pd.DataFrame:
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
    return get_result_by_query_id(connection, run_query(connection, query))


def run_query(connection: Connection, query: dict) -> str:
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
            f"Accepted {query} at {connection.url}/api/{connection.api_version}with id {query_id}"
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


def daily_location(
    date: str,
    aggregation_unit: str,
    daily_location_method: str,
    subscriber_subset: Union[dict, None] = None,
) -> dict:
    """
    Return query spec for a daily location query for a date and unit of aggregation.

    Parameters
    ----------
    date : str
        ISO format date to get the daily location for, e.g. "2016-01-01"
    aggregation_unit : str
        Unit of aggregation, e.g. "admin3"
    daily_location_method : str
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
    if subscriber_subset is None:
        subscriber_subset = "all"
    return {
        "query_kind": "daily_location",
        "params": {
            "date": date,
            "aggregation_unit": aggregation_unit,
            "daily_location_method": daily_location_method,
            "subscriber_subset": subscriber_subset,
        },
    }


def modal_location(
    *daily_locations: Dict[str, Union[str, Dict[str, str]]], aggregation_unit: str
) -> dict:
    """
    Return query spec for a modal location query for a list of daily locations.

    Parameters
    ----------
    daily_locations : list of dicts
        List of daily location query specifications
    aggregation_unit : str
        Unit of aggregation, e.g. "admin3"

    Returns
    -------
    dict
        Dict which functions as the query specification for the modal location

    """
    return {
        "query_kind": "modal_location",
        "params": {"locations": daily_locations, "aggregation_unit": aggregation_unit},
    }


def modal_location_from_dates(
    start_date: str,
    stop_date: str,
    aggregation_unit: str,
    daily_location_method: str,
    subscriber_subset: Union[dict, None] = None,
) -> dict:
    """
    Return query spec for a modal location query for an (inclusive) date range and unit of aggregation.

    Parameters
    ----------
    start_date : str
        ISO format date that begins the period, e.g. "2016-01-01"
    stop_date : str
        ISO format date that begins the period, e.g. "2016-01-07"
    aggregation_unit : str
        Unit of aggregation, e.g. "admin3"
    daily_location_method : str
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
        d.strftime("%Y-%m-%d") for d in pd.date_range(start_date, stop_date, freq="D")
    ]
    daily_locations = [
        daily_location(
            date,
            aggregation_unit=aggregation_unit,
            daily_location_method=daily_location_method,
            subscriber_subset=subscriber_subset,
        )
        for date in dates
    ]
    return modal_location(*daily_locations, aggregation_unit=aggregation_unit)


def flows(
    from_location: Dict[str, Union[str, Dict[str, str]]],
    to_location: Dict[str, Union[str, Dict[str, str]]],
    aggregation_unit: str,
) -> dict:
    """
    Return query spec for flows between two locations.

    Parameters
    ----------
    from_location: dict
        Query which maps individuals to single location for the "origin" period of interest.
    to_location: dict
        Query which maps individuals to single location for the "destination" period of interest.
    aggregation_unit : str
        Unit of aggregation, e.g. "admin3"

    Returns
    -------
    dict
        Dict which functions as the query specification for the flow

    """
    return {
        "query_kind": "flows",
        "params": {
            "from_location": from_location,
            "to_location": to_location,
            "aggregation_unit": aggregation_unit,
        },
    }
