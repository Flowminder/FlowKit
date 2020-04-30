# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from typing import Union

import requests

import flowclient.connection


class ASyncConnection(flowclient.connection.Connection):
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

    async def get_url(
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
        return super().get_url(route=route, data=data)

    async def post_json(self, *, route: str, data: dict) -> requests.Response:
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
        return super().post_json(route=route, data=data)

    def make_api_query(self, parameters: dict) -> "ASyncAPIQuery":
        from flowclient.async_api_query import ASyncAPIQuery

        return ASyncAPIQuery(connection=self, parameters=parameters)

    def __repr__(self):
        return f"{super().__repr__()} (async)"
