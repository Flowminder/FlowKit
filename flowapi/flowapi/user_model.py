from json import JSONEncoder

from flask_jwt_extended.exceptions import UserClaimsVerificationError
from typing import Dict, Union, List

from flask_jwt_extended import get_jwt_claims, get_jwt_identity
from quart import current_app, request


class UserObject(JSONEncoder):
    """
    Class to represent a user's permissions as loaded from a JWT.

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
        self, *, action: str, query_kind: str, aggregation_unit: str
    ) -> bool:
        """
        Returns true if the user can do 'action' with this kind of query at this unit of aggregation.

        Parameters
        ----------
        action: {'run', 'poll', 'get_results'}
            Action to check
        query_kind : str
            Kind of the query
        aggregation_unit : str
            Aggregation unit/level of resolution

        Returns
        -------
        bool
            True if the user can do 'action' with this query

        """
        try:
            action_rights = self.claims[query_kind]["permissions"][action]
            unit_rights = (
                aggregation_unit in self.claims[query_kind]["spatial_aggregation"]
            )
            if not action_rights:
                raise UserClaimsVerificationError(
                    f"Token does not allow {action} for query kind '{query_kind}'"
                )
            if not unit_rights:
                raise UserClaimsVerificationError(
                    f"Token does not allow query kind '{query_kind}' at spatial aggregation '{aggregation_unit}'"
                )
        except KeyError:
            raise UserClaimsVerificationError("Claims verification failed.")
        return True

    def can_run(self, *, query_kind: str, aggregation_unit: str) -> bool:
        """
        Returns true if the user can run this kind of query at this unit of aggregation.
        Parameters
        ----------
        query_kind : str
            Kind of the query
        aggregation_unit : str
            Aggregation unit/level of resolution

        Returns
        -------
        bool
            True if the user can run this query

        """

        return self.has_access(
            action="run", query_kind=query_kind, aggregation_unit=aggregation_unit
        )

    def can_poll(self, *, query_kind: str, aggregation_unit: str) -> bool:
        """
        Returns true if the user can poll this kind of query at this unit of aggregation.
        Parameters
        ----------
        query_kind : str
            Kind of the query
        aggregation_unit : str
            Aggregation unit/level of resolution

        Returns
        -------
        bool
            True if the user can poll this query

        """

        return self.has_access(
            action="poll", query_kind=query_kind, aggregation_unit=aggregation_unit
        )

    def can_get_results(self, *, query_kind: str, aggregation_unit: str) -> bool:
        """
        Returns true if the user can get the results of this kind of query at this unit of aggregation.
        Parameters
        ----------
        query_kind : str
            Kind of the query
        aggregation_unit : str
            Aggregation unit/level of resolution

        Returns
        -------
        bool
            True if the user can get the results of this query

        """

        return self.has_access(
            action="get_result",
            query_kind=query_kind,
            aggregation_unit=aggregation_unit,
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

        """

        return self.has_access(
            action="get_result",
            query_kind="geography",
            aggregation_unit=aggregation_unit,
        )


def user_loader_callback(identity):
    current_app.access_logger.info(
        "Attempting to load user",
        request_id=request.request_id,
        route=request.path,
        user=get_jwt_identity(),
        src_ip=request.headers.get("Remote-Addr"),
    )

    try:
        claims = get_jwt_claims()
    except KeyError:
        return None

    log_dict = dict(
        request_id=request.request_id,
        route=request.path,
        user=get_jwt_identity(),
        src_ip=request.headers.get("Remote-Addr"),
        claims=claims,
    )
    current_app.query_run_logger.info("Loaded user", **log_dict)

    return UserObject(username=identity, claims=claims)
