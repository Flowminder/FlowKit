import logging
from copy import deepcopy

import redis
from json import dumps, loads, JSONDecodeError


from flowmachine.core import Query, GeoTable
from flowmachine.core.cache import get_query_object_by_id, cache_table_exists
from flowmachine.core.query_state import QueryStateMachine, QueryState
from flowmachine.features import (
    daily_location,
    ModalLocation,
    Flows,
    TotalLocationEvents,
    MeaningfulLocations,
    HartiganCluster,
    CallDays,
    EventScore,
    MeaningfulLocationsOD,
    MeaningfulLocationsAggregate,
)
from flowmachine.features.utilities.subscriber_locations import subscriber_locations

logger = logging.getLogger("flowmachine").getChild(__name__)


query_class_map = {
    # "modal_location": ModalLocation,
    "daily_location": daily_location,
    # "flow": Flows,
    # "custom_query": CustomQuery,
    # "most_frequent_location": MostFrequentLocation,
    # "last_location": LastLocation,
    # "radius_of_gyration": RadiusOfGyration,
}


class RedisLookupError(Exception):
    """
    Custom exception to indicate that a redis lookup
    (e.g. of query_id -> query_descr) failed.
    """


class RedisInterface:
    """
    Wrapper class to encapsulate interactions with redis.
    The main purpose of this is to allow easier testing,
    especially when locks are involved.
    """

    def __init__(self, redis):
        self._redis = redis

    def get(self, key):
        try:
            return self._redis.get(key)
        except redis.exceptions.ConnectionError:
            raise QueryProxyError("Cannot establish connection to redis")

    def set(self, key, value):
        return self._redis.set(key, value)

    def keys(self):
        return self._redis.keys()

    def query_status(self, query_id: str) -> QueryState:
        return QueryStateMachine(self._redis, query_id).current_query_state


class QueryProxyError(Exception):
    """
    Custom exception to indicate an error related to QueryProxy,
    for example if construction of a query object failed or a
    query id doesn't exist even though it should.
    """


class InvalidGeographyError(Exception):
    """
    Custom exception to indicate that the aggregation unit for a
    geography query is invalid (corresponds to a 404 error in the API).
    """


class MissingQueryError(Exception):
    """
    Custom exception to indicate that a query for a given query id doesn't exist.
    """

    def __init__(self, missing_query_id, *, msg):
        super().__init__(msg)
        self.missing_query_id = missing_query_id


def construct_query_object(query_kind, params):  # pragma: no cover
    """
    Create an instance of the appropriate subclass of flowmachine.core.query.Query

    Parameters
    ----------
    query_kind : str
        The kind of query to be constructed. Example: "daily_location".

    params : dict
        Parameters to use in the query construction.

    Returns
    -------
    flowmachine.core.query.Query
    """
    params = deepcopy(
        params
    )  # Operate on a copy to avoid mutating the passed in dict, which might change the redis lookup
    error_msg_prefix = (
        f"Error when constructing query of kind {query_kind} with parameters {params}"
    )
    try:
        subscriber_subset = params["subscriber_subset"]
        if subscriber_subset == "all":
            params["subscriber_subset"] = None
        else:
            if isinstance(subscriber_subset, dict):
                raise NotImplementedError("Proper subsetting not implemented yet.")
            else:
                raise QueryProxyError(
                    f"{error_msg_prefix}: 'Cannot construct {query_kind} subset from given input: {subscriber_subset}'"
                )
    except KeyError:
        pass  # No subset param

    if "daily_location" == query_kind:
        date = params["date"]
        method = params["daily_location_method"]
        level = params["aggregation_unit"]
        subscriber_subset = params["subscriber_subset"]

        allowed_methods = ["last", "most-common"]
        allowed_levels = ["admin0", "admin1", "admin2", "admin3", "admin4"]

        if method not in allowed_methods:
            raise QueryProxyError(
                f"{error_msg_prefix}: 'Unrecognised method '{method}', must be one of: {allowed_methods}'"
            )

        if level not in allowed_levels:
            raise QueryProxyError(
                f"{error_msg_prefix}: 'Unrecognised level '{level}', must be one of: {allowed_levels}'"
            )

        try:
            q = daily_location(
                date=date,
                method=method,
                level=level,
                subscriber_subset=subscriber_subset,
            )
        except Exception as e:
            raise QueryProxyError(f"{error_msg_prefix}: '{e}'")
    elif "location_event_counts" == query_kind:
        start_date = params["start_date"]
        end_date = params["end_date"]
        interval = params["interval"]
        level = params["aggregation_unit"]
        subscriber_subset = params["subscriber_subset"]
        direction = params["direction"]
        event_types = params["event_types"]

        allowed_intervals = TotalLocationEvents.allowed_intervals
        allowed_directions = ["in", "out", "all"]
        allowed_levels = [
            "admin0",
            "admin1",
            "admin2",
            "admin3",
            "admin4",
            "site",
            "cell",
        ]

        if interval not in allowed_intervals:
            raise QueryProxyError(
                f"{error_msg_prefix}: 'Unrecognised interval '{interval}', must be one of: {allowed_intervals}'"
            )

        if level not in allowed_levels:
            raise QueryProxyError(
                f"{error_msg_prefix}: 'Unrecognised level '{level}', must be one of: {allowed_levels}'"
            )

        if level in ["cell", "site"]:
            level = f"versioned-{level}"

        if direction not in allowed_directions:
            raise QueryProxyError(
                f"{error_msg_prefix}: 'Unrecognised direction '{direction}', must be one of: {allowed_directions}'"
            )
        if direction == "all":
            direction = "both"

        try:
            q = TotalLocationEvents(
                start=start_date,
                stop=end_date,
                direction=direction,
                table=event_types,
                level=level,
                subscriber_subset=subscriber_subset,
            )
            logger.debug(f"Made TotalLocationEvents query. {q.__dict__}")
        except Exception as e:
            raise QueryProxyError(f"{error_msg_prefix}: '{e}'")
    elif "modal_location" == query_kind:
        locations = params["locations"]
        aggregation_unit = params["aggregation_unit"]
        try:
            location_objects = []
            for loc in locations:
                query_kind = loc["query_kind"]
                if query_kind != "daily_location":
                    raise QueryProxyError(
                        f"{error_msg_prefix}: Currently modal location takes only daily locations as input."
                    )
                if aggregation_unit != loc["params"]["aggregation_unit"]:
                    raise QueryProxyError(
                        f"{error_msg_prefix}: Modal location aggregation unit must be the same as the ones of all input locations."
                    )
                params = loc["params"]
                dl = construct_query_object(query_kind, params)
                location_objects.append(dl)
            q = ModalLocation(*location_objects)
        except Exception as e:
            raise QueryProxyError(f"{error_msg_prefix}: '{e}'")

    elif "flows" == query_kind:
        aggregation_unit = params["aggregation_unit"]
        try:
            from_location = params["from_location"]
            to_location = params["to_location"]
            if (
                aggregation_unit != from_location["params"]["aggregation_unit"]
                or aggregation_unit != to_location["params"]["aggregation_unit"]
            ):
                raise QueryProxyError(
                    f"{error_msg_prefix}: Flow aggregation unit must be the same as the ones for from_location and to_location."
                )
            from_location_object = construct_query_object(
                from_location["query_kind"], from_location["params"]
            )
            to_location_object = construct_query_object(
                to_location["query_kind"], to_location["params"]
            )
            q = Flows(from_location_object, to_location_object)
        except Exception as e:
            raise QueryProxyError(f"FIXME (flows): {e}")

    elif "meaningful_locations_aggregate" == query_kind:
        aggregation_unit = params["aggregation_unit"]
        mfl = params["meaningful_locations"]
        try:
            q = MeaningfulLocationsAggregate(
                meaningful_locations=construct_query_object(**mfl),
                level=aggregation_unit,
            )
        except Exception as e:
            raise QueryProxyError(f"FIXME (meaningful_location_aggregate): {e}")

    elif "meaningful_locations_od_matrix" == query_kind:
        aggregation_unit = params["aggregation_unit"]
        mfl_a = params["meaningful_locations_a"]
        mfl_b = params["meaningful_locations_b"]
        try:
            q = MeaningfulLocationsOD(
                meaningful_locations_a=construct_query_object(**mfl_a),
                meaningful_locations_b=construct_query_object(**mfl_b),
                level=aggregation_unit,
            )
        except Exception as e:
            raise QueryProxyError(f"FIXME (meaningful_location_od_matrix): {e}")

    elif "meaningful_locations" == query_kind:
        label = params["label"]
        scores = params["scores"]
        labels = params["labels"]
        clusters = params["clusters"]
        try:
            q = MeaningfulLocations(
                clusters=construct_query_object(**clusters),
                labels=labels,
                scores=construct_query_object(**scores),
                label=label,
            )
        except Exception as e:
            raise QueryProxyError(f"FIXME (meaningful_locations): {e}")
    elif "event_score" == query_kind:
        try:
            q = EventScore(**params)
        except Exception as e:
            raise QueryProxyError(f"FIXME (event_score): {e}")

    elif "hartigan_cluster" == query_kind:
        call_days = params.pop("call_days")
        try:
            q = HartiganCluster(calldays=construct_query_object(**call_days), **params)
        except Exception as e:
            raise QueryProxyError(f"FIXME (hartigan_cluster): {e}")

    elif "call_days" == query_kind:
        sls = params.pop("subscriber_locations")
        try:
            q = CallDays(subscriber_locations=construct_query_object(**sls))
        except Exception as e:
            raise QueryProxyError(f"FIXME (call_days): {e}")
    elif "subscriber_locations" == query_kind:
        try:
            q = subscriber_locations(**params)
        except Exception as e:
            raise QueryProxyError(f"FIXME (subscriber_locations): {e}")
    elif "geography" == query_kind:
        aggregation_unit = params["aggregation_unit"]

        allowed_aggregation_units = ["admin0", "admin1", "admin2", "admin3", "admin4"]

        if aggregation_unit not in allowed_aggregation_units:
            raise InvalidGeographyError(
                f"{error_msg_prefix}: 'Unrecognised aggregation unit '{aggregation_unit}', "
                f"must be one of: {allowed_aggregation_units}'"
            )

        try:
            q = GeoTable(
                name=aggregation_unit,
                schema="geography",
                columns=[f"{aggregation_unit}name", f"{aggregation_unit}pcod", "geom"],
            )
        except Exception as e:
            raise QueryProxyError(f"{error_msg_prefix}: '{e}'")

    else:
        error_msg = f"Unsupported query kind: '{query_kind}'"
        logger.error(error_msg)
        raise QueryProxyError(error_msg)

    logger.debug(f"Made {query_kind}: {params}")
    return q


def get_sql_for_query_id(query_id):
    """
    Return the SQL which, when run against flowdb, will
    return the result for the query with the given id.

    Parameters
    ----------
    query_id : str
        The query id

    Returns
    -------
    str
    """
    q = get_query_object_by_id(Query.connection, query_id)
    sql = q.get_query()
    return sql


class QueryProxy:
    """
    This class acts as the interface and "translator" between the
    JSON representation of a query (via `query_kind` and `params`)
    and the actual flowmachine.core.query.Query object.

    It is responsible for storing the lookup between query_id and
    a string representing (query_kind, params) in redis and for
    constructing the flowmachine.core.query.Query if needed.
    """

    def __init__(
        self, query_kind, params, *, redis=None, func_construct_query_object=None
    ):
        if not isinstance(query_kind, str):
            raise QueryProxyError(
                f"Argument 'query_kind' must be of type str, got: '{type(query_kind)}'"
            )
        if not isinstance(params, dict):
            raise QueryProxyError(
                f"Argument 'params' must be of type dict, got: '{type(params)}'"
            )

        self.query_kind = query_kind
        self.params = params
        self.redis_interface = RedisInterface(redis=(redis or Query.redis))
        self.func_construct_query_object = (
            func_construct_query_object or construct_query_object
        )
        self._query_descr = dumps(
            {"query_kind": query_kind, "params": params}, sort_keys=True
        )

    @classmethod
    def from_query_id(cls, query_id, *, redis=None):
        redis = redis or Query.redis
        query_descr = redis.get(query_id)
        if query_descr is None:
            raise MissingQueryError(query_id, msg=f"Unknown query id: {query_id}")
        else:
            try:
                query_kind = loads(query_descr)["query_kind"]
                params = loads(query_descr)["params"]
            except JSONDecodeError:
                raise QueryProxyError(
                    "Query description does not contain valid JSON: '{query_descr}'. This should never happen."
                )
            return QueryProxy(query_kind, params, redis=redis)

    def _get_query_id_from_redis(self):
        query_id = self.redis_interface.get(self._query_descr)
        if query_id is not None:
            logger.debug(
                f"Query of kind {self.query_kind} with params {self.params} is already known."
            )
            # Note that redis stores strings as raw bytes, so we need to decode it here.
            # It might be possible let redis do this by setting `decode_responses=True`
            # when creating the redis.StrictRedis instance in connect(), but this caused
            # subtle bugs when I tried it last time so better to explicitly do the decode
            # here for the time being.
            return query_id.decode()
        else:
            raise RedisLookupError(
                f"No redis entry exists for query of kind {self.query_kind} with params {self.params}"
            )

    def _create_redis_lookup(self, query_id):
        self.redis_interface.set(self._query_descr, query_id)
        self.redis_interface.set(query_id, self._query_descr)

    def run_query_async(self):
        """
        Trigger an async store of a query, and return the resulting query id
        which can be used for polling the query and obtaining the result.

        Returns
        -------
        str

        """
        logger.debug(
            f"Scheduling query of kind {self.query_kind} with params {self.params}"
        )
        logger.debug(
            f"Checking if {self.query_kind} with params {self.params} is already known."
        )

        try:
            query_id = self._get_query_id_from_redis()
        except RedisLookupError:
            q = self.func_construct_query_object(self.query_kind, self.params)
            q.store()
            try:
                # In addition to the actual query, also set an aggregated version running.
                # This is the one which we return via the API so that we don't expose any
                # individual-level data.
                q_agg = q.aggregate()
                q_agg.store()
                query_id = q_agg.md5
            except AttributeError:
                # This can happen for flows, which doesn't support aggregation
                query_id = q.md5
            self._create_redis_lookup(query_id)
            logger.debug(f"Triggered store for query {query_id}")

        return query_id

    def poll(self):
        """
        Return the status of a submitted query.

        Returns
        -------
        str

        """
        query_id = self._get_query_id_from_redis()
        logger.debug(f"Getting status for query {query_id} of kind {self.query_kind}")

        status = self.redis_interface.query_status(query_id)
        if status == QueryState.EXECUTED and not cache_table_exists(
            Query.connection, query_id
        ):
            return "awol"

        return status

    def get_sql(self):
        """
        For a query which has been completed, return the SQL code which, when run against flowdb, returns the output.

        Returns
        -------
        str

        """
        query_id = self._get_query_id_from_redis()
        try:
            query_state = self.redis_interface.query_status(query_id)
        except ValueError as e:
            raise QueryProxyError(
                f"Got a bad state for '{query_id}'. Original exception was {e}"
            )

        if query_state == QueryState.EXECUTING:
            raise QueryProxyError(f"Query with id '{query_id}' is still running.")
        elif query_state == QueryState.QUEUED:
            raise QueryProxyError(f"Query with id '{query_id}' is still queued.")
        elif query_state == QueryState.ERRORED:
            raise QueryProxyError(f"Query with id '{query_id}' is failed.")
        elif query_state == QueryState.CANCELLED:
            raise QueryProxyError(f"Query with id '{query_id}' was cancelled.")
        elif query_state == QueryState.RESETTING:
            raise QueryProxyError(
                f"Query with id '{query_id}' is being removed from cache."
            )
        elif query_state == QueryState.KNOWN:
            raise MissingQueryError(
                query_id,
                msg=f"Query with id '{query_id}' has not been run yet, or was reset.",
            )
        elif query_state == QueryState.EXECUTED:
            return get_sql_for_query_id(query_id)
        else:
            raise QueryProxyError(
                f"Unknown state for query with id '{query_id}'. Got {query_state}."
            )
