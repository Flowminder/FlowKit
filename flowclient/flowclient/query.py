# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


from typing import Union

from flowclient.client import (
    FlowclientConnectionError,
    Connection,
    run_query,
    get_status,
    get_result_by_query_id,
    get_geojson_result_by_query_id,
    wait_for_query_to_be_ready,
)


class Query:
    """
    Representation of a FlowKit query, including parameters and result.

    Parameters
    ----------
    connection : Connection
        Connection to FlowKit server on which to run this query
    parameters : dict
        Parameters that specify the query

    Attributes
    ----------
    parameters
    connection
    status
    """

    def __init__(self, *, connection: Connection, parameters: dict):
        self.connection = connection
        self.parameters = dict(
            parameters
        )  # TODO: make this a property? (For immutability; otherwise parameters could differ from query ID / result)

    def run(self) -> None:
        """
        Set this query running in FlowKit

        Raises
        ------
        FlowclientConnectionError
            if the query cannot be set running
        """
        self._query_id = run_query(
            connection=self.connection, query_spec=self.parameters
        )
        # TODO: Return a future?

    @property
    def connection(self) -> Connection:
        """
        Connection that is used for running this query.

        Returns
        -------
        Connection
            Connection to FlowKit API
        """
        return self._connection

    @connection.setter
    def connection(self, new_connection: Connection) -> None:
        """
        Set a new connection (e.g. to replace an expired token).
        Note: if the URL of the new connection differs from that of the existing connection,
        the query result will be invalidated.

        Parameters
        ----------
        new_connection : Connection
            New API connection to use with this query.
        """
        if hasattr(self, "_connection") and new_connection.url != self._connection.url:
            # If new URL is for a different API, invalidate query ID and result
            # TODO: Or should we disallow this, and instead add a Connection.update_token method?
            try:
                delattr(self, "_query_id")
            except AttributeError:
                pass
        self._connection = new_connection

    @property
    def status(self) -> str:
        """
        Status of this query.

        Returns
        -------
        str
            One of:
            - "not running"
            - "queued"
            - "executing"
            - "completed"
        """
        if not hasattr(self, "_query_id"):
            return "not running"
        status, _ = get_status(connection=self.connection, query_id=self._query_id)
        return status

    def get_result(
        self, format: str = "pandas", poll_interval: int = 1
    ) -> Union["pandas.DataFrame", dict]:
        """
        Get the result of this query, as a pandas DataFrame or GeoJSON dict.

        Parameters
        ----------
        format : str
            Result format. One of {'pandas', 'geojson'}
        poll_interval : int
            Number of seconds to wait between checks for the query being ready

        Returns
        -------
        pandas.DataFrame or dict
            Query result
        """
        if format == "pandas":
            result_getter = get_result_by_query_id
        elif format == "geojson":
            result_getter = get_geojson_result_by_query_id
        else:
            raise ValueError(
                f"Invalid format: {format}. Expected one of {{'pandas', 'geojson'}}."
            )

        # TODO: Cache result internally?
        try:
            # TODO: Warn if not yet completed?
            return result_getter(
                connection=self.connection,
                query_id=self._query_id,
                poll_interval=poll_interval,
            )
        except (AttributeError, FlowclientConnectionError):
            # TODO: Warn before running?
            self.run()
            return result_getter(
                connection=self.connection,
                query_id=self._query_id,
                poll_interval=poll_interval,
            )

    def wait_until_ready(self, poll_interval: int = 1) -> None:
        """
        Wait until this query has finished running.

        Parameters
        ----------
        poll_interval : int
            Number of seconds to wait between checks for the query being ready

        Raises
        ------
        FlowclientConnectionError
            if query is not running or has errored
        """
        if not hasattr(self, "_query_id"):
            raise FileNotFoundError("Query is not running.")
        wait_for_query_to_be_ready(
            connection=self.connection,
            query_id=self._query_id,
            poll_interval=poll_interval,
        )
