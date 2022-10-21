# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from itertools import permutations
from typing import List

from quart_jwt_extended import get_jwt_claims, get_jwt_identity
from quart_jwt_extended.exceptions import UserClaimsVerificationError

from flowapi.flowapi_errors import BadQueryError, MissingQueryKindError
from flowapi.permissions import query_to_scopes
from flowapi.utils import get_query_parameters_from_flowmachine
from quart import current_app, request


class UserObject:
    """
    Class to represent a user's permissions as loaded from a JWT.
    Provided methods which check a user has access to perform specific actions
    against API endpoints.

    Parameters
    ----------
    username : str
        Name of the user
    scopes : dict
        Dictionary giving a whitelist of the user's claims
    """

    def __init__(self, username: str, scopes: dict) -> None:
        self.username = username
        self.scopes = scopes

    def has_access(
        self, *, actions: List[str] = None, claims: List[str] = None
    ) -> bool:
        if not actions or claims:
            raise ValueError("has_access needs at least actions or claims")
        # try:
        #     claims = set(await query_to_scopes(query_json))
        # except Exception as exc:
        #     raise BadQueryError
        # if "query_kind" not in query_json:
        #     raise MissingQueryKindError
        granting_roles = []
        for role, scopes in self.scopes.items():
            if all(x in scopes for x in {*claims, *actions}):
                granting_roles.append(role)
        if granting_roles:
            current_app.access_logger.info(
                f"Permission for {actions} over {claims} granted by {granting_roles}"
            )
            return True
        raise UserClaimsVerificationError

    async def can_run(self, *, query_json: dict) -> bool:
        """
        Returns true if the user can run this query.

        Parameters
        ----------
        query_json : str
            Query json

        Returns
        -------
        bool
            True if the user can run this query

        Raises
        ------
        UserClaimsVerificationError
            If the user cannot run this kind of query at this level of aggregation

        """
        claims = await query_to_scopes(query_json)
        return self.has_access(actions=["run"], claims=claims)

    async def can_poll_by_query_id(self, *, query_id) -> bool:
        """
        Returns true if the user can poll this query.

        Parameters
        ----------
        query_id : str
            Identifier of the query.

        Returns
        -------
        bool
            True if the user can get the status of this query

        Raises
        ------
        UserClaimsVerificationError
            If the user cannot get the status of this kind of query at this level of aggregation
        """

        params = await get_query_parameters_from_flowmachine(query_id=query_id)
        return self.can_poll(query_json=params)

    async def can_poll(self, *, query_json: dict) -> bool:
        """
        Returns true if the user can poll this kind of query at this unit of aggregation.

        Parameters
        ----------
        query_json : dict
            Query spec

        Returns
        -------
        bool
            True if the user can poll this query

        Raises
        ------
        UserClaimsVerificationError
            If the user cannot get the status of this kind of query at this level of aggregation
        """
        claims = await query_to_scopes(query_json)
        return self.has_access(claims=claims)

    async def can_get_results_by_query_id(self, *, query_id) -> bool:
        """
        Returns true if the user can get the results of this query.

        Parameters
        ----------
        query_id : str
            Identifier of the query.

        Returns
        -------
        bool
            True if the user can get the results of this query

        Raises
        ------
        UserClaimsVerificationError
            If the user cannot get the results of this kind of query at this level of aggregation
        """
        params = await get_query_parameters_from_flowmachine(query_id=query_id)
        return self.can_get_results(query_json=params)

    async def can_get_results(self, *, query_json: dict) -> bool:
        """
        Returns true if the user can get the results of this kind of query at this unit of aggregation.
        Parameters
        ----------
        query_json : dict
            Query spec

        Returns
        -------
        bool
            True if the user can get the results of this query

        Raises
        ------
        UserClaimsVerificationError
            If the user cannot get the results of this kind of query at this level of aggregation
        """
        claims = await query_to_scopes(query_json)
        return self.has_access(actions=["get_result"], claims=claims)

    def can_get_geography(self, *, aggregation_unit: str) -> bool:
        """
        Returns true if the user can get this geography as geojson.
        .
        Parameters
        ----------
        aggregation_unit : str
            Aggregation unit/level of resolution

        Returns
        -------
        bool
            True if the user can get this geography

        Raises
        ------
        UserClaimsVerificationError
            If the user get geography at this level
        """
        return self.has_access(
            actions=["get_result"],
            claims=[f"{aggregation_unit}:geography:geography"],
        )

    def can_get_available_dates(self) -> bool:
        return self.has_access(actions=["get_available_dates"])


def user_loader_callback(identity):
    """
    Call back for loading user from JWT.

    Parameters
    ----------
    identity : str
        Username

    Returns
    -------
    UserObject
        User with claims pulled from the decoded jwt token

    """
    current_app.access_logger.info(
        "Attempting to load user",
        request_id=request.request_id,
        route=request.path,
        user=get_jwt_identity(),
        src_ip=request.headers.get("Remote-Addr"),
    )
    # claims = decompress_claims(get_jwt_claims())
    claims = get_jwt_claims()

    log_dict = dict(
        request_id=request.request_id,
        route=request.path,
        user=get_jwt_identity(),
        src_ip=request.headers.get("Remote-Addr"),
        claims=claims,
    )
    current_app.access_logger.info("Loaded user", **log_dict)
    return UserObject(username=identity, scopes=claims)
