# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from abc import ABCMeta, abstractmethod
from copy import deepcopy

from flowmachine.core import Query
from flowmachine.core.query_info_lookup import QueryInfoLookup

__all__ = ["BaseExposedQuery"]


class BaseExposedQuery(metaclass=ABCMeta):
    """
    Base class for exposed flowmachine queries.

    Note: this class and derived classes are not meant to be instantiated directly!
    Instead, they are instantiated automatically by the class FlowmachineQuerySchema.

    Example:

        FlowmachineQuerySchema().load({"query_kind": "dummy_query", "dummy_param": "foobar"})
    """

    @property
    @abstractmethod
    def _flowmachine_query_obj(self):
        """
        Return the underlying flowmachine query object which this class exposes.

        Returns
        -------
        Query
        """
        raise NotImplementedError(
            f"Class {self.__class__.__name__} does not have the fm_query_obj property set."
        )

    def store_async(self):
        """
        Store this query using a background thread.

        Returns
        -------
        Future
            Future object representing the calculation.

        """
        q = self._flowmachine_query_obj
        q.store()

        # FIXME: currently we also aggregate the query here, but this is not
        # the place to do this. Instead, there should be a separate query_kind
        # to perform the spatial aggregation. Once we have this, the following
        # code block should be removed and simply `q.md5` be returned as the
        # query_id.
        try:
            # In addition to the actual query, also set an aggregated version running.
            # This is the one which we return via the API so that we don't expose any
            # individual-level data.
            q_agg = q.aggregate()
            q_agg.store()
            query_id = q_agg.md5
            # FIXME: this is a big hack to register the query info lookup also
            # for the aggregated query. This should not be necessary any more once
            # this whole code block is removed (see fixme comment above). To make
            # it obvious that it is the lookup of an aggregated query we artificially
            # insert two additional keys 'is_aggregate' and 'non_aggregated_query_id'.
            q_agg_info_lookup = QueryInfoLookup(Query.redis)
            query_params_agg = deepcopy(self.query_params)
            query_params_agg["is_aggregate"] = True
            query_params_agg["non_aggregated_query_id"] = self.query_id
            q_agg_info_lookup.register_query(q_agg.md5, self.query_params)
        except AttributeError:
            # This can happen for flows, which doesn't support aggregation
            query_id = q.md5

        return query_id

    @property
    def query_id(self):
        # TODO: Ideally we'd like to return the md5 hash of the query parameters
        # as known to the marshmallow schema:
        #    return md5(json.dumps(self.query_params, sort_keys=True).encode()).hexdigest()
        #
        # However, the resulting md5 hash is different from the one produced internally
        # by flowmachine.core.Query.md5, and the latter is currently being used by
        # the QueryStateMachine, so we need to use it to check the query state.
        return self._flowmachine_query_obj.md5

    @property
    def query_params(self):
        """
        Return the parameters from which the query is constructed. Note that this
        includes the the parameters of any subqueries of which it is composed.

        Returns
        -------
        dict
            JSON representation of the query parameters, including those of subqueries.
        """
        # We need to dynamically import FlowmachineQuerySchema here in order to avoid
        # a circular import in this file because currently there are dependencies of
        # the following form (using the example of DailyLocation):
        #
        #     DailyLocationSchema -> DailyLocationExposed -> BaseExposedQuery -> FlowmachineQuerySchema -> DailyLocationSchema
        #
        # We break the dependency BaseExposedQuery -> FlowmachineQuerySchema here by using
        # this dynamic import. There is possibly a better way to do this...
        from flowmachine.core.server.query_schemas import FlowmachineQuerySchema

        return FlowmachineQuerySchema().dump(self)

    def _create_query_info_lookup(self):
        """
        Create the query info lookup between query_id and query parameters, so that
        we can get information about the query later using only its query_id.
        """
        # TODO: is this the right place for this or should this happen somewhere else?!
        q_info_lookup = QueryInfoLookup(Query.redis)
        q_info_lookup.register_query(self.query_id, self.query_params)
