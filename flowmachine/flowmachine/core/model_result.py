# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
ModelResult provides machinery to interface with results created outside
of postgres.



"""


from concurrent.futures import Future
from time import sleep

from typing import List, Union

import pandas as pd
from sqlalchemy.engine import Engine

from flowmachine.core.cache import write_query_to_cache
from flowmachine.core.errors.flowmachine_errors import (
    QueryCancelledException,
    QueryErroredException,
)
from flowmachine.core.query_state import QueryStateMachine
from flowmachine.core.query import Query

import structlog

logger = structlog.get_logger("flowmachine.debug", submodule=__name__)


class ModelResult(Query):
    """
    Class representing a result calculated outside of the database.


    Parameters
    ----------
    parent : Model
        The Model this is a run of
    run_args : list, optional
        List of arguments passed in the Model.run() method
    run_kwargs : dict, optional
        List of named arguments passed in the Model.run() method
    df : pandas.DataFrame, optional
        Results of model.run()
    """

    def __init__(self, parent, run_args=None, run_kwargs=None, df=None):
        self.model_dependencies, self.model_args = self._split_query_objects(parent)
        self.parent_class = parent.__class__.__name__
        self.run_args = run_args
        self.run_kwargs = run_kwargs
        if df is not None:
            self._df = df

        super().__init__()

    def __repr__(self):
        rargs = ", ".join(
            ["{!r}".format(x) for x in self.run_args]
            + ["{}={!r}".format(k, v) for k, v in self.run_kwargs.items()]
        )
        return "Model result of type {cl}: run({rargs})".format(
            cl=self.parent_class, rargs=rargs
        )

    @staticmethod
    def _split_query_objects(obj):
        """
        Return a tuple of lists, one containing Query objects in obj
        the other (attribute-name, value) tuples.

        Parameters
        ----------
        obj : Object
            Object to extract from
        Returns
        -------
        tuple of lists
        """

        qs = []
        args = []
        openlist = list(obj.__dict__.items())
        while openlist:
            k, o = openlist.pop()
            if isinstance(o, Query):
                qs.append(o)
            elif isinstance(o, list):
                openlist += zip([k] * len(o), o)
            elif isinstance(o, dict):
                openlist += list(o.items())
            elif not isinstance(o, pd.DataFrame):
                args.append((k, o))
        return qs, args

    def __iter__(self):
        if self.is_stored:
            return super().__iter__()
        else:
            self._query_object = self._df.iterrows()
        return self

    def __next__(self):
        if self.is_stored:
            return super().__next__()
        else:
            return self._query_object.__next__()

    def __len__(self):
        try:
            return len(self._df)
        except AttributeError:
            return super().__len__()

    @property
    def column_names(self) -> List[str]:
        try:
            return self._df.columns.tolist()
        except AttributeError:
            return super().column_names

    def to_sql(
        self,
        name: str,
        schema: Union[str, None] = None,
        store_dependencies: bool = False,
    ) -> Future:
        """
        Store the result of the calculation back into the database.

        Parameters
        ----------
        name : str
            name of the table
        schema : str, default None
            Name of an existing schema. If none will use the postgres default,
            see postgres docs for more info.
        store_dependencies : bool, default False
            Required for compatibility with query.to_sql
            store_dependencies=True raises NotImplementedError

        Returns
        -------
        Future
            Future object, containing this query and any result information.
        
        Raises
        ------
        NotImplementedError
            If store_dependencies = True

        Notes
        -----

        This method will return a Future immediately.
        """
        if store_dependencies:
            raise NotImplementedError(
                "ModelResult does not support storing dependencies."
            )

        if not self.is_stored:
            try:
                self._df
            except AttributeError:
                raise ValueError("Not computed yet.")

        def write_model_result(query_ddl_ops: List[str], connection: Engine) -> float:
            self._df.to_sql(name, connection, schema=schema, index=False)
            QueryStateMachine(self.redis, self.query_id).finish()
            return self._runtime

        current_state, changed_to_queue = QueryStateMachine(
            self.redis, self.query_id
        ).enqueue()
        logger.debug(
            f"Attempted to enqueue query '{self.query_id}', query state is now {current_state} and change happened {'here and now' if changed_to_queue else 'elsewhere'}."
        )
        # name, redis, query, connection, ddl_ops_func, write_func, schema = None, sleep_duration = 1
        store_future = self.thread_pool_executor.submit(
            write_query_to_cache,
            name=name,
            schema=schema,
            query=self,
            connection=self.connection,
            redis=self.redis,
            ddl_ops_func=lambda *x: [],
            write_func=write_model_result,
        )
        return store_future

    def _make_query(self):
        if not self.is_stored:
            self.store().result()
        return self.get_query()
