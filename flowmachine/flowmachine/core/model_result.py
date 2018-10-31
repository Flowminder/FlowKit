# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
ModelResult provides machinery to interface with results created outside
of postgres.



"""

import logging
from typing import List

import pandas as pd

from flowmachine.utils.utils import rlock
from .query import Query

logger = logging.getLogger("flowmachine").getChild(__name__)


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

    def to_sql_async(
        self, name=None, schema=None, as_view=False, as_temp=False, force=False
    ):
        """
        Store the result of the calculation back into the database.

        Parameters
        ----------
        name : str
            name of the table
        schema : str, default None
            Name of an existing schema. If none will use the postgres default,
            see postgres docs for more info.
        as_view : bool, default False
            Set to True to store as a view rather than a table. A view
            is always up to date even if the underlying data changes.
        as_temp : bool, default False
            Set to true to store this only for the duration of the
            interpreter session
        force : bool, default False
            Will overwrite an existing table if the name already exists

        Returns
        -------
        Future
            Future object, containing this query and any result information.

        Notes
        -----

        This method will return a Future immediately.
        """

        if not self.is_stored:
            try:
                self._df
            except AttributeError:
                raise ValueError("Not computed yet.")

        def do_query():
            logger.debug("Getting storage lock.")
            with rlock(self.redis, self.md5):
                logger.debug("Obtained storage lock.")
                con = self.connection.engine
                if force and not as_view:
                    self.invalidate_db_cache(name, schema=schema)
                try:
                    with con.begin():
                        logger.debug("Using pandas to store.")
                        self._df.to_sql(name, con, schema=schema, index=False)
                        if not as_view and schema == "cache":
                            self._db_store_cache_metadata()
                except AttributeError:
                    logger.debug(
                        "No dataframe to store, presumably because this"
                        " was retrieved from the db."
                    )
            logger.debug("Released storage lock.")
            return self

        store_future = self.tp.submit(do_query)
        return store_future

    def _make_query(self):
        if not self.is_stored:
            self.store().result()
        return self.get_query()
