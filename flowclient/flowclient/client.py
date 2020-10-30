# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import logging
import re

import httpx
import pandas as pd
import time
from typing import Tuple, Union, List, Optional
from tqdm.auto import tqdm

from flowclient.connection import Connection
from flowclient.errors import FlowclientConnectionError

logger = logging.getLogger(__name__)


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
) -> Tuple[bool, httpx.Response]:
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
    *,
    connection: Connection,
    query_id: str,
    poll_interval: int = 1,
    disable_progress: Optional[bool] = None,
) -> httpx.Response:
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
    disable_progress : bool, default None
        Set to True to disable progress bar display entirely, None to disable on
        non-TTY, or False to always enable

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

    if not query_ready:
        progress = reply.json()["progress"]
        total_eligible = progress["eligible"]
        completed = 0
        with tqdm(
            desc="Parts run", disable=disable_progress, unit="q", total=total_eligible
        ) as total_bar:
            while not query_ready:
                logger.info("Waiting before polling again.")
                time.sleep(
                    poll_interval
                )  # Wait a second, then check if the query is ready again
                query_ready, reply = query_is_ready(
                    connection=connection, query_id=query_id
                )  # Poll the server
                if query_ready:
                    break
                else:
                    progress = reply.json()["progress"]
                    completion_change = (
                        total_eligible - progress["eligible"]
                    ) - completed
                    completed += completion_change

                total_bar.update(completion_change)

            total_bar.update(total_eligible - completed)  # Finish the bar

    return reply


def get_result_location_from_id_when_ready(
    *,
    connection: Connection,
    query_id: str,
    poll_interval: int = 1,
    disable_progress: Optional[bool] = None,
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
    disable_progress : bool, default None
        Set to True to disable progress bar display entirely, None to disable on
        non-TTY, or False to always enable

    Returns
    -------
    str
        Endpoint to retrieve results from

    """
    reply = wait_for_query_to_be_ready(
        connection=connection,
        query_id=query_id,
        poll_interval=poll_interval,
        disable_progress=disable_progress,
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
    *,
    connection: Connection,
    query_id: str,
    poll_interval: int = 1,
    disable_progress: Optional[bool] = None,
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
    disable_progress : bool, default None
        Set to True to disable progress bar display entirely, None to disable on
        non-TTY, or False to always enable

    Returns
    -------
    dict
        geojson

    """
    result_endpoint = get_result_location_from_id_when_ready(
        connection=connection,
        query_id=query_id,
        poll_interval=poll_interval,
        disable_progress=disable_progress,
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
    *,
    connection: Connection,
    query_id: str,
    poll_interval: int = 1,
    disable_progress: Optional[bool] = None,
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
    disable_progress : bool, default None
        Set to True to disable progress bar display entirely, None to disable on
        non-TTY, or False to always enable

    Returns
    -------
    pandas.DataFrame
        Dataframe containing the result

    """
    result_endpoint = get_result_location_from_id_when_ready(
        connection=connection,
        query_id=query_id,
        poll_interval=poll_interval,
        disable_progress=disable_progress,
    )
    return get_json_dataframe(connection=connection, location=result_endpoint)


def get_geojson_result(
    *,
    connection: Connection,
    query_spec: dict,
    disable_progress: Optional[bool] = None,
) -> dict:
    """
    Run and retrieve a query of a specified kind with parameters.

    Parameters
    ----------
    connection : Connection
        API connection to use
    query_spec : dict
        A query specification to run, e.g. `{'kind':'daily_location', 'params':{'date':'2016-01-01'}}`
    disable_progress : bool, default None
        Set to True to disable progress bar display entirely, None to disable on
        non-TTY, or False to always enable

    Returns
    -------
    dict
       Geojson

    """
    return get_geojson_result_by_query_id(
        connection=connection,
        query_id=run_query(connection=connection, query_spec=query_spec),
        disable_progress=disable_progress,
    )


def get_result(
    *,
    connection: Connection,
    query_spec: dict,
    disable_progress: Optional[bool] = None,
) -> pd.DataFrame:
    """
    Run and retrieve a query of a specified kind with parameters.

    Parameters
    ----------
    connection : Connection
        API connection to use
    query_spec : dict
        A query specification to run, e.g. `{'kind':'daily_location', 'date':'2016-01-01'}`
    disable_progress : bool, default None
        Set to True to disable progress bar display entirely, None to disable on
        non-TTY, or False to always enable

    Returns
    -------
    pd.DataFrame
       Pandas dataframe containing the results

    """
    return get_result_by_query_id(
        connection=connection,
        query_id=run_query(connection=connection, query_spec=query_spec),
        disable_progress=disable_progress,
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
