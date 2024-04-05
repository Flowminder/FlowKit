# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import datetime
import logging
import re
from asyncio import sleep

import httpx
import pandas as pd
from typing import Tuple, Union, List, Optional, Dict
from tqdm.auto import tqdm


import flowclient.errors
from flowclient.async_connection import ASyncConnection

logger = logging.getLogger(__name__)


async def connect_async(
    *,
    url: str,
    token: str,
    api_version: int = 0,
    ssl_certificate: Union[str, bool] = True,
) -> ASyncConnection:
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
    ssl_certificate: str or bool
        Provide a path to an ssl certificate to use, True to use
        default root certificates, or False to disable ssl verification.

    Returns
    -------
    ASyncConnection
    """
    return ASyncConnection(
        url=url, token=token, api_version=api_version, ssl_certificate=ssl_certificate
    )


async def query_is_ready(
    *, connection: ASyncConnection, query_id: str
) -> Tuple[bool, httpx.Response]:
    """
    Check if a query id has results available.

    Parameters
    ----------
    connection : ASyncConnection
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
    reply = await connection.get_url(route=f"poll/{query_id}")

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
        raise flowclient.errors.FlowclientConnectionError(
            f"Something went wrong: {reply}. API returned with status code: {reply.status_code}"
        )


async def get_status(*, connection: ASyncConnection, query_id: str) -> str:
    """
    Check the status of a query.

    Parameters
    ----------
    connection : ASyncConnection
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
        ready, reply = await query_is_ready(connection=connection, query_id=query_id)
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
            raise flowclient.errors.FlowclientConnectionError(f"No status reported.")


async def wait_for_query_to_be_ready(
    *,
    connection: ASyncConnection,
    query_id: str,
    poll_interval: int = 1,
    disable_progress: Optional[bool] = None,
) -> httpx.Response:
    """
    Wait until a query id has finished running, and if it finished successfully
    return the reply from flowapi.

    Parameters
    ----------
    connection : ASyncConnection
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
    query_ready, reply = await query_is_ready(
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
                await sleep(
                    poll_interval
                )  # Wait a second, then check if the query is ready again
                query_ready, reply = await query_is_ready(
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


async def get_result_location_from_id_when_ready(
    *,
    connection: ASyncConnection,
    query_id: str,
    poll_interval: int = 1,
    disable_progress: Optional[bool] = None,
) -> str:
    """
    Return, once ready, the location at which results of a query will be obtainable.

    Parameters
    ----------
    connection : ASyncConnection
        API connection  to use
    query_id : str
        Identifier of the query to retrieve
    poll_interval : int
        Number of seconds to wait between checks for the query being ready
    disable_progress : bool, async default None
        Set to True to disable progress bar display entirely, None to disable on
        non-TTY, or False to always enable

    Returns
    -------
    str
        Endpoint to retrieve results from

    """
    reply = await wait_for_query_to_be_ready(
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


async def get_json_dataframe(
    *, connection: ASyncConnection, location: str
) -> pd.DataFrame:
    """
    Get a dataframe from a json source.

    Parameters
    ----------
    connection : ASyncConnection
        API connection  to use
    location : str
        API enpoint to retrieve json from

    Returns
    -------
    pandas.DataFrame
        Dataframe containing the result

    """

    response = await connection.get_url(route=location)
    if response.status_code != 200:
        try:
            msg = response.json()["msg"]
            more_info = f" Reason: {msg}"
        except KeyError:
            more_info = ""
        raise flowclient.errors.FlowclientConnectionError(
            f"Could not get result. API returned with status code: {response.status_code}.{more_info}"
        )
    result = response.json()
    logger.info(f"Got {connection.url}/api/{connection.api_version}/{location}")
    return pd.DataFrame.from_records(result["query_result"])


async def get_geojson_result_by_query_id(
    *,
    connection: ASyncConnection,
    query_id: str,
    poll_interval: int = 1,
    disable_progress: Optional[bool] = None,
) -> dict:
    """
    Get a query by id, and return it as a geojson dict

    Parameters
    ----------
    connection : ASyncConnection
        API connection  to use
    query_id : str
        Identifier of the query to retrieve
    poll_interval : int
        Number of seconds to wait between checks for the query being ready
    disable_progress : bool, async default None
        Set to True to disable progress bar display entirely, None to disable on
        non-TTY, or False to always enable

    Returns
    -------
    dict
        geojson

    """
    result_endpoint = await get_result_location_from_id_when_ready(
        connection=connection,
        query_id=query_id,
        poll_interval=poll_interval,
        disable_progress=disable_progress,
    )
    response = await connection.get_url(route=f"{result_endpoint}.geojson")
    if response.status_code != 200:
        try:
            msg = response.json()["msg"]
            more_info = f" Reason: {msg}"
        except KeyError:
            more_info = ""
        raise flowclient.errors.FlowclientConnectionError(
            f"Could not get result. API returned with status code: {response.status_code}.{more_info}"
        )
    return response.json()


async def get_result_by_query_id(
    *,
    connection: ASyncConnection,
    query_id: str,
    poll_interval: int = 1,
    disable_progress: Optional[bool] = None,
) -> pd.DataFrame:
    """
    Get a query by id, and return it as a dataframe

    Parameters
    ----------
    connection : ASyncConnection
        API connection  to use
    query_id : str
        Identifier of the query to retrieve
    poll_interval : int
        Number of seconds to wait between checks for the query being ready
    disable_progress : bool, async default None
        Set to True to disable progress bar display entirely, None to disable on
        non-TTY, or False to always enable

    Returns
    -------
    pandas.DataFrame
        Dataframe containing the result

    """
    result_endpoint = await get_result_location_from_id_when_ready(
        connection=connection,
        query_id=query_id,
        poll_interval=poll_interval,
        disable_progress=disable_progress,
    )
    return await get_json_dataframe(connection=connection, location=result_endpoint)


async def get_geojson_result(
    *,
    connection: ASyncConnection,
    query_spec: dict,
    disable_progress: Optional[bool] = None,
) -> dict:
    """
    Run and retrieve a query of a specified kind with parameters.

    Parameters
    ----------
    connection : ASyncConnection
        API connection to use
    query_spec : dict
        A query specification to run, e.g. `{'kind':'daily_location', 'params':{'date':'2016-01-01'}}`
    disable_progress : bool, async default None
        Set to True to disable progress bar display entirely, None to disable on
        non-TTY, or False to always enable

    Returns
    -------
    dict
       Geojson

    """
    return await get_geojson_result_by_query_id(
        connection=connection,
        query_id=await run_query(connection=connection, query_spec=query_spec),
        disable_progress=disable_progress,
    )


async def get_result(
    *,
    connection: ASyncConnection,
    query_spec: dict,
    disable_progress: Optional[bool] = None,
) -> pd.DataFrame:
    """
    Run and retrieve a query of a specified kind with parameters.

    Parameters
    ----------
    connection : ASyncConnection
        API connection to use
    query_spec : dict
        A query specification to run, e.g. `{'kind':'daily_location', 'date':'2016-01-01'}`
    disable_progress : bool, async default None
        Set to True to disable progress bar display entirely, None to disable on
        non-TTY, or False to always enable

    Returns
    -------
    pd.DataFrame
       Pandas dataframe containing the results

    """
    return await get_result_by_query_id(
        connection=connection,
        query_id=await run_query(connection=connection, query_spec=query_spec),
        disable_progress=disable_progress,
    )


async def get_geography(*, connection: ASyncConnection, aggregation_unit: str) -> dict:
    """
    Get geography data from the database.

    Parameters
    ----------
    connection : ASyncConnection
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
    response = await connection.get_url(route=f"geography/{aggregation_unit}")
    if response.status_code != 200:
        try:
            msg = response.json()["msg"]
            more_info = f" Reason: {msg}"
        except KeyError:
            more_info = ""
        raise flowclient.errors.FlowclientConnectionError(
            f"Could not get result. API returned with status code: {response.status_code}.{more_info}"
        )
    result = response.json()
    logger.info(
        f"Got {connection.url}/api/{connection.api_version}/geography/{aggregation_unit}"
    )
    return result


async def get_available_dates(
    *, connection: ASyncConnection, event_types: Union[None, List[str]] = None
) -> dict:
    """
    Get available dates for different event types from the database.

    Parameters
    ----------
    connection : ASyncConnection
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
    response = await connection.get_url(route=f"available_dates")
    if response.status_code != 200:
        try:
            msg = response.json()["msg"]
            more_info = f" Reason: {msg}"
        except KeyError:
            more_info = ""
        raise flowclient.errors.FlowclientConnectionError(
            f"Could not get available dates. API returned with status code: {response.status_code}.{more_info}"
        )
    result = response.json()["available_dates"]
    logger.info(f"Got {connection.url}/api/{connection.api_version}/available_dates")
    if event_types is None:
        return result
    else:
        return {k: v for k, v in result.items() if k in event_types}


async def get_available_qa_checks(
    *, connection: ASyncConnection, event_types: Union[None, List[str]] = None
) -> List[Dict[str, str]]:
    """
    Get available QA checks for some or all event types.

    Parameters
    ----------
    connection : ASyncConnection
        API connection to use
    event_types : list of str, optional
        The event types for which to return available checks (for example: ["calls", "sms"]).
        If None, return available checks for all available event types.

    Returns
    -------
    list of dict
        Available checks in the format {"cdr_type": <event_type>, "type_of_query_or_check":<check_type>}

    """
    logger.info(f"Getting {connection.url}/api/{connection.api_version}/qa/")
    response = await connection.get_url(route=f"qa/")
    if response.status_code != 200:
        try:
            msg = response.json()["msg"]
            more_info = f" Reason: {msg}"
        except KeyError:
            more_info = ""
        raise flowclient.errors.FlowclientConnectionError(
            f"Could not get qa checks. API returned with status code: {response.status_code}.{more_info}"
        )
    result = response.json()
    logger.info(f"Got {connection.url}/api/{connection.api_version}/qa/")
    if event_types is None:
        return result
    else:
        return [check for check in result if check["cdr_type"] in event_types]


async def get_available_qa_checks_df(
    *, connection: ASyncConnection, event_types: Union[None, List[str]] = None
) -> pd.DataFrame:
    """
    Get available QA checks for some or all event types as a dataframe.

    Parameters
    ----------
    connection : ASyncConnection
        API connection to use
    event_types : list of str, optional
        The event types for which to return available checks (for example: ["calls", "sms"]).
        If None, return available checks for all available event types.

    Returns
    -------
    pd.DataFrame
        Available checks as a dataframe

    """
    return pd.DataFrame(
        await get_available_qa_checks(connection=connection, event_types=event_types)
    )


async def get_qa_check_outcome(
    *,
    connection: ASyncConnection,
    event_type: str,
    check_type: str,
    event_date: Union[datetime.date, str],
) -> str:
    """
    Get the outcome of a single QA check on a specified date and event type.

    Parameters
    ----------
    connection : ASyncConnection
        API connection to use
    event_type : str
        The event type for which to return the outcome.
    check_type : str
        The type of check to get the outcome for.
    event_date : datetime.date or str
        The event date to get the QA outcome for.

    Returns
    -------
    str
        The result of the check as a string value

    """
    logger.info(
        f"Getting {connection.url}/api/{connection.api_version}/qa/{event_type}/{check_type}//{event_date}"
    )
    response = await connection.get_url(
        route=f"qa/{event_type}/{check_type}/{event_date}"
    )
    if response.status_code != 200:
        try:
            msg = response.json()["msg"]
            more_info = f" Reason: {msg}"
        except KeyError:
            more_info = ""
        raise flowclient.errors.FlowclientConnectionError(
            f"Could not get qa check outcome. API returned with status code: {response.status_code}.{more_info}"
        )
    result = response.text
    logger.info(
        f"Got {connection.url}/api/{connection.api_version}/qa/{event_type}/{check_type}//{event_date}"
    )
    return result


async def get_qa_check_outcomes(
    *,
    connection: ASyncConnection,
    event_type: str,
    check_type: str,
    start_date: Union[datetime.date, str],
    end_date: Union[datetime.date, str],
) -> List[Dict[str, str]]:
    """
    Get the outcome of a single QA check on one event type over a time range.

    Parameters
    ----------
    connection : ASyncConnection
        API connection to use
    event_type : str
        The event type for which to return the outcome.
    check_type : str
        The type of check to get the outcome for.
    start_date, end_date : datetime.date or str
        The event date to get QA outcomes between (end date inclusive).

    Returns
    -------
    list of dict
        QA check outputs in the format {"cdr_date": <event_date>, "type_of_query_or_check":<check_type>, "outcome": <outcome_of_check>}

    """
    logger.info(
        f"Getting {connection.url}/api/{connection.api_version}/qa/{event_type}/{check_type}/",
        params=dict(start_date=start_date, end_date=end_date),
    )
    response = await connection.get_url(
        route=f"qa/{event_type}/{check_type}",
        params=dict(start_date=start_date, end_date=end_date),
    )
    if response.status_code != 200:
        try:
            msg = response.json()["msg"]
            more_info = f" Reason: {msg}"
        except KeyError:
            more_info = ""
        raise flowclient.errors.FlowclientConnectionError(
            f"Could not get qa check outcome. API returned with status code: {response.status_code}.{more_info}"
        )
    result = response.json()
    logger.info(
        f"Got {connection.url}/api/{connection.api_version}/qa/{event_type}/{check_type}/",
        params=dict(start_date=start_date, end_date=end_date),
    )
    return result


async def get_qa_check_outcomes_df(
    *,
    connection: ASyncConnection,
    event_type: str,
    check_type: str,
    start_date: Union[datetime.date, str],
    end_date: Union[datetime.date, str],
) -> pd.DataFrame:
    """
    Get the outcome of a single QA check on one event type over a time range as a dataframe

    Parameters
    ----------
    connection : ASyncConnection
        API connection to use
    event_type : str
        The event type for which to return the outcome.
    check_type : str
        The type of check to get the outcome for.
    start_date, end_date : datetime.date or str
        The event date to get QA outcomes between (end date inclusive).

    Returns
    -------
    pd.DataFrame
        QA check outputs with columns cdr_date, type_of_query_or_check, and outcome

    """
    return pd.DataFrame(
        await get_qa_check_outcomes(
            connection=connection,
            event_type=event_type,
            check_type=check_type,
            start_date=start_date,
            end_date=end_date,
        )
    )


async def run_query(*, connection: ASyncConnection, query_spec: dict) -> str:
    """
    Run a query of a specified kind with parameters and get the identifier for it.

    Parameters
    ----------
    connection : ASyncConnection
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
    r = await connection.post_json(route="run", data=query_spec)
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
        raise flowclient.errors.FlowclientConnectionError(
            f"Error running the query: {error}. Status code: {r.status_code}."
        )
