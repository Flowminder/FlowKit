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
            f"Class {self.__class__.__name__} does not have the _flowmachine_query_obj property set."
        )

    def store_async(self, store_dependencies=True):
        """
        Store this query using a background thread.

        Parameters
        ----------
        store_dependencies : bool, default True
            If True, set the dependencies of this query running first.

        Returns
        -------
        str
            Query ID that can be used to check the query state.
        """
        q = self._flowmachine_query_obj

        q.store(store_dependencies=store_dependencies)
        query_id = q.query_id

        return query_id

    @property
    def query_id(self):
        # TODO: Ideally we'd like to return the md5 hash of the query parameters
        # as known to the marshmallow schema:
        #    return md5(json.dumps(self.query_params, sort_keys=True).encode()).hexdigest()
        #
        # However, the resulting md5 hash is different from the one produced internally
        # by flowmachine.core.Query.query_id, and the latter is currently being used by
        # the QueryStateMachine, so we need to use it to check the query state.
        return self._flowmachine_query_obj.query_id
