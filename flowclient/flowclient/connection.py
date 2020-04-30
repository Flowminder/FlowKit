# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
import logging
import warnings
from typing import Union

import jwt
import requests
from requests import ConnectionError
from flowclient.errors import FlowclientConnectionError

logger = logging.getLogger(__name__)


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

    def make_api_query(self, parameters: dict) -> "APIQuery":
        from flowclient.api_query import APIQuery

        return APIQuery(connection=self, parameters=parameters)

    def __repr__(self) -> str:
        return f"{self.user}@{self.url} v{self.api_version}"
