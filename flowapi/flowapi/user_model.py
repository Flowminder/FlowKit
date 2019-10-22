# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


from flask_jwt_extended.exceptions import UserClaimsVerificationError
from typing import List, Tuple, Set, FrozenSet

from flask_jwt_extended import get_jwt_claims, get_jwt_identity
from quart import current_app, request

from flowapi.permissions import query_to_scope_set, scopes_to_sets
from flowapi.utils import get_query_parameters_from_flowmachine


class UserObject:
    """
    Class to represent a user's permissions as loaded from a JWT.
    Provided methods which check a user has access to perform specific actions
    against API endpoints.

    Parameters
    ----------
    username : str
        Name of the user
    claims : dict
        Dictionary giving a whitelist of the user's claims
    """

    def __init__(self, username: str, scope_set: Set[FrozenSet]) -> None:
        self.username = username
        self.scope_set = scope_set

    def can_run(self, *, query_json: dict) -> bool:
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

        return query_to_scope_set(dict(action="run", **query_json)) in self.scope_set

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

    def can_poll(self, *, query_json: dict) -> bool:
        """
        Returns true if the user can poll this kind of query at this unit of aggregation.

        Parameters
        ----------
        query_json : list of tuples
            List of tuples giving a query kind and aggregation unit

        Returns
        -------
        bool
            True if the user can poll this query

        Raises
        ------
        UserClaimsVerificationError
            If the user cannot get the status of this kind of query at this level of aggregation
        """

        return (
            query_to_scope_set(dict(action="run", **query_json)) in self.scope_set
            or query_to_scope_set(dict(action="get_result", **query_json))
            in self.scope_set
        )

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

    def can_get_results(self, *, query_json: dict) -> bool:
        """
        Returns true if the user can get the results of this kind of query at this unit of aggregation.
        Parameters
        ----------
        query_kinds_and_aggregations : list of tuples
            List of tuples giving a query kind and aggregation unit

        Returns
        -------
        bool
            True if the user can get the results of this query

        Raises
        ------
        UserClaimsVerificationError
            If the user cannot get the results of this kind of query at this level of aggregation
        """

        return (
            query_to_scope_set(dict(action="get_result", **query_json))
            in self.scope_set
        )

    def can_get_geography(self, *, aggregation_unit: str) -> bool:
        """
        Returns true if the user can get the this geography as geojson.
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

        return (
            query_to_scope_set(
                dict(
                    action="get_result",
                    query_kind="geography",
                    aggregation_unit=aggregation_unit,
                )
            )
            in self.scope_set
        )

    def can_get_available_dates(self) -> bool:
        return (
            query_to_scope_set(dict(action="get_result", query_kind="available_dates"))
            in self.scope_set
        )


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

    claims = get_jwt_claims()

    log_dict = dict(
        request_id=request.request_id,
        route=request.path,
        user=get_jwt_identity(),
        src_ip=request.headers.get("Remote-Addr"),
        claims=claims,
    )
    current_app.access_logger.info("Loaded user", **log_dict)

    return UserObject(username=identity, scope_set=scopes_to_sets(claims))
