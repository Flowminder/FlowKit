# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


from flask_jwt_extended.exceptions import UserClaimsVerificationError
from quart.exceptions import HTTPException
from typing import Dict, Union, List, Tuple

from flask_jwt_extended import get_jwt_claims, get_jwt_identity
from quart import current_app, request

from flowapi.flowapi_errors import (
    MissingQueryKindError,
    MissingAggregationUnitError,
    BadQueryError,
)


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

    def __init__(
        self,
        username: str,
        claims: Dict[str, Dict[str, Union[List[str], Dict[str, bool]]]],
    ) -> None:
        self.username = username
        self.claims = claims

    def has_access(
        self, *, action: str, query_kinds_and_aggregations: List[Tuple[str, str]]
    ) -> bool:
        """
        Returns true if the user can do 'action' with this kind of query at this unit of aggregation.

        Parameters
        ----------
        action: {'run', 'poll', 'get_results'}
            Action to check
        query_kinds_and_aggregations : list of tuples
            List of tuples giving a query kind and aggregation unit

        Returns
        -------
        bool
            True if the user can do 'action' with this query

        Raises
        ------
        UserClaimsVerificationError
            If the user cannot do action with this kind of query at this level of aggregation
        """
        for query_kind, aggregation_unit in query_kinds_and_aggregations:
            try:
                action_rights = self.claims[query_kind]["permissions"][action]
                aggregation_right = (
                    aggregation_unit in self.claims[query_kind]["spatial_aggregation"]
                )
                if not action_rights:
                    raise UserClaimsVerificationError(
                        f"Token does not allow {action} for query kind '{query_kind}'"
                    )
                if not aggregation_right:
                    raise UserClaimsVerificationError(
                        f"Token does not allow query kind '{query_kind}' at spatial aggregation '{aggregation_unit}'"
                    )
            except KeyError:
                raise UserClaimsVerificationError("Claims verification failed.")
        return True

    def _get_query_kinds_and_aggregation_units(
        self, query_json: dict
    ) -> List[Tuple[str, str]]:
        """
        Extract the query kind and aggregation unit from a query spec dict.

        Parameters
        ----------
        query_json : dict
            Dictionary containing a query spec

        Returns
        -------
        list of tuples str, str
            List of tuples giving query kind and aggregation unit

        """

        try:
            query_kind = query_json["query_kind"]
        except KeyError:
            raise MissingQueryKindError
        try:
            if query_kind == "spatial_aggregate":
                return self._get_query_kinds_and_aggregation_units(
                    query_json=query_json["locations"]
                )
            elif query_kind == "joined_spatial_aggregate":
                location_spec = self._get_query_kinds_and_aggregation_units(
                    query_json=query_json["locations"]
                )[0]
                metric_spec = self._get_query_kinds_and_aggregation_units(
                    query_json=dict(
                        **query_json["metric"], aggregation_unit=location_spec[1]
                    )
                )[0]
                return [location_spec, metric_spec]
        except (KeyError, SyntaxError):
            raise BadQueryError
        try:
            aggregation_unit = query_json["aggregation_unit"]
        except KeyError:
            raise MissingAggregationUnitError
        return [(query_kind, aggregation_unit)]

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
        query_kinds_and_aggregations = self._get_query_kinds_and_aggregation_units(
            query_json=query_json
        )

        return self.has_access(
            action="run", query_kinds_and_aggregations=query_kinds_and_aggregations
        )

    @staticmethod
    async def _get_params(*, query_id) -> dict:
        """
        Get the parameters of a query from flowmachine.

        Parameters
        ----------
        query_id : str
            ID of the query to get params for

        Returns
        -------
        dict
            Dictionary containing the query's original parameters

        Raises
        ------
        HTTPException
            404 if the query id is not known.

        """
        request.socket.send_json(
            {
                "request_id": request.request_id,
                "action": "get_query_params",
                "params": {"query_id": query_id},
            }
        )
        reply = await request.socket.recv_json()
        if reply["status"] == "error":
            raise HTTPException(
                description=f"Unknown query ID '{query_id}'",
                name="Query ID not found",
                status_code=404,
            )
        return reply["payload"]["query_params"]

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

        params = await self._get_params(query_id=query_id)
        query_kinds_and_aggregations = self._get_query_kinds_and_aggregation_units(
            query_json=params
        )
        return self.can_poll(query_kinds_and_aggregations=query_kinds_and_aggregations)

    def can_poll(self, *, query_kinds_and_aggregations: List[Tuple[str, str]]) -> bool:
        """
        Returns true if the user can poll this kind of query at this unit of aggregation.

        Parameters
        ----------
        query_kinds_and_aggregations : list of tuples
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

        return self.has_access(
            action="poll", query_kinds_and_aggregations=query_kinds_and_aggregations
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
        params = await self._get_params(query_id=query_id)
        query_kinds_and_aggregations = self._get_query_kinds_and_aggregation_units(
            query_json=params
        )
        return self.can_get_results(
            query_kinds_and_aggregations=query_kinds_and_aggregations
        )

    def can_get_results(
        self, *, query_kinds_and_aggregations: List[Tuple[str, str]]
    ) -> bool:
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

        return self.has_access(
            action="get_result",
            query_kinds_and_aggregations=query_kinds_and_aggregations,
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

        return self.has_access(
            action="get_result",
            query_kinds_and_aggregations=[("geography", aggregation_unit)],
        )

    def can_get_available_dates(self) -> bool:
        allowed_to_access_available_dates = (
            self.claims.get("available_dates", {})
            .get("permissions", {})
            .get("get_result", False)
        )
        if not allowed_to_access_available_dates:
            raise UserClaimsVerificationError(
                f"Token does not allow access to available dates."
            )
        return True


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

    return UserObject(username=identity, claims=claims)
