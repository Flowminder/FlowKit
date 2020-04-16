# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


from typing import Union, Optional

from flowclient.client import (
    run_query,
    get_status,
    get_result_by_query_id,
    get_geojson_result_by_query_id,
    wait_for_query_to_be_ready,
)
from flowclient.connection import Connection


class APIQuery:
    """
    Representation of a FlowKit query.

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
        self._connection = connection
        self.parameters = dict(parameters)

    def __repr__(self) -> str:
        return f"<{self.__class__.__module__}.{self.__class__.__name__} object, connection={self.connection}, parameters={self.parameters}>"

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

    @property
    def status(self) -> str:
        """
        Status of this query.

        Returns
        -------
        str
            One of:
            - "not_running"
            - "queued"
            - "executing"
            - "completed"
        """
        if not hasattr(self, "_query_id"):
            return "not_running"
        return get_status(connection=self.connection, query_id=self._query_id)

    def get_result(
        self,
        format: str = "pandas",
        poll_interval: int = 1,
        disable_progress: Optional[bool] = None,
    ) -> Union["pandas.DataFrame", dict]:
        """
        Get the result of this query, as a pandas DataFrame or GeoJSON dict.

        Parameters
        ----------
        format : str, default 'pandas'
            Result format. One of {'pandas', 'geojson'}
        poll_interval : int, default 1
            Number of seconds to wait between checks for the query being ready
        disable_progress : bool, default None
            Set to True to disable progress bar display entirely, None to disable on
            non-TTY, or False to always enable

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
                f"Invalid format: '{format}'. Expected one of {{'pandas', 'geojson'}}."
            )

        # TODO: Cache result internally?
        try:
            return result_getter(
                connection=self.connection,
                query_id=self._query_id,
                poll_interval=poll_interval,
                disable_progress=disable_progress,
            )
        except (AttributeError, FileNotFoundError):
            # TODO: Warn before running?
            self.run()
            return result_getter(
                connection=self.connection,
                query_id=self._query_id,
                poll_interval=poll_interval,
                disable_progress=disable_progress,
            )

    def wait_until_ready(
        self, poll_interval: int = 1, disable_progress: Optional[bool] = None
    ) -> None:
        """
        Wait until this query has finished running.

        Parameters
        ----------
        poll_interval : int, default 1
            Number of seconds to wait between checks for the query being ready
        disable_progress : bool, default None
            Set to True to disable progress bar display entirely, None to disable on
            non-TTY, or False to always enable


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
            disable_progress=disable_progress,
        )
