# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from typing import List

import numpy as np
import pandas as pd

from abc import abstractmethod
from sqlalchemy.sql import ClauseElement, select, text, column
from .query import Query

__all__ = [
    "make_subscriber_subsetter",
    "SubscriberSubsetterForAllSubscribers",
    "SubscriberSubsetterForExplicitSubset",
    "SubscriberSubsetterForFlowmachineQuery",
]


class SubscriberSubsetterBase(Query):
    """
    Base class for the different types of subscriber subsets.

    TODO: this class currently inherits from flowmachine.Query, mainly for
    practical reasons (to avoid changes to the subsetting logic ripple
    through the entire codebase because of the way the caching logic works).
    Unfortunately, this requires us to implement `_make_query()` and
    `_get_query_attrs_for_dependency_graph`. In the long run we should
    remove the inheritance from Query, which will allow us to remove these
    stub implementations too.
    """

    @property
    @abstractmethod
    def is_proper_subset(self):
        raise NotImplementedError(
            f"Class {self.__class__.__name__} does not implement 'is_proper_subset'"
        )

    def column_names(self) -> List[str]:
        return []

    @abstractmethod
    def _make_query(self):
        raise NotImplementedError(
            f"Class {self.__class__.__name__} does not implement '_make_query'"
        )

    @abstractmethod
    def apply_subset_if_needed(self, sql, *, subscriber_identifier):
        raise NotImplementedError(
            f"Class {self.__class__.__name__} does not implement 'apply_subset'"
        )

    def _get_query_attrs_for_dependency_graph(self, analyse=False):
        # This is a stub implementation of this internal method.
        # It is only needed because SubscriberSubsetterBase currently
        # inherits from flowmachine.Query and this helper method is
        # called from Query.dependency_graph(). However, since this
        # class doesn't really represent a full flowmachine.Query
        # (and this inheritance will be removed in the long run)
        # we can't return meaningful values here, so we just return
        # a dictionary with the correct keys but no actual values.
        attrs = {}
        attrs["name"] = self.__class__.__name__
        attrs["stored"] = "N/A"
        attrs["cost"] = "N/A"
        attrs["runtime"] = "N/A"
        return attrs


class SubscriberSubsetterForAllSubscribers(SubscriberSubsetterBase):
    """
    Represents the subset of all subscribers - i.e., no subsetting at all.
    In other words this is the "null object" for the subsetting logic, which
    represents the case of "no work needed".

    The reason this exists is so that external code does not need to know
    anything about the subsetting logic or implementation and can use any
    of these classes completely interchangeably, no matter what kind
    of subset is required or whether any subsetting is needed at all.

    If we didn't have this class then external code would need to make
    a case distinction to check if subsetting is needed, which leads
    to coupling between unrelated parts of the code base, unnecessary
    complexity and makes testing of all possible cases much difficult.
    """

    is_proper_subset = False

    def _make_query(self):
        # Return a dummy string representing this subset. This is only needed
        # because SubscriberSubsetterBase currently inherits from Query, but will
        # eventually be removed.
        return "<SubscriberSubsetterForAllSubscribers>"

    def apply_subset_if_needed(self, sql, *, subscriber_identifier=None):
        """
        Return the input query unchanged, since no subsetting is applied.

        Parameters
        ----------
        sql : sqlalchemy.sql.ClauseElement
            The SQL query to which the subset should be applied.

        subscriber_identifier : str
            This argument is ignored for subsets of type 'AllSubscribers'.
        """
        return sql


class SubscriberSubsetterForFlowmachineQuery(SubscriberSubsetterBase):
    """
    Represents a subset given by a flowmachine query.
    """

    is_proper_subset = True

    def __init__(self, flowmachine_query):
        """
        Parameters
        ----------
        flowmachine_query : flowmachine.Query
            The flowmachine query to be used for subsetting. The only requirement
            on it is that the result has a column called "subscriber" (it is fine
            for other columns to be present, too).
        """
        assert isinstance(flowmachine_query, Query)

        self._verify_that_subscriber_column_is_present(flowmachine_query)
        self.flowmachine_query = flowmachine_query
        super().__init__()

    def _verify_that_subscriber_column_is_present(self, flowmachine_query):
        """
        Check that the flowmachine query contains a 'subscriber' column and
        raise an error if this is not the case.
        """
        if "subscriber" not in flowmachine_query.column_names:
            raise ValueError(
                f"Flowmachine query used for subsetting must contain a 'subscriber' column. "
                f"Columns present are: {flowmachine_query.column_names}"
            )

    def _make_query(self):
        # Return a dummy string representing this subset. This is only needed
        # because SubscriberSubsetterBase currently inherits from Query, but will
        # eventually be removed.
        return "<SubscriberSubsetterForFlowmachineQuery>"

    def apply_subset_if_needed(self, sql, *, subscriber_identifier=None):
        """
        Return a modified version of the input SQL query which has the subset applied.

        Parameters
        ----------
        sql : sqlalchemy.sql.ClauseElement
            The SQL query to which the subset should be applied.

        subscriber_identifier : str
            This argument is ignored for subsets of type 'SubscriberSubsetterForFlowmachineQuery'.

        Returns
        ----------
        sqlalchemy.sql.ClauseElement
        """
        assert isinstance(sql, ClauseElement)

        tbl = sql.alias("tbl")

        # Create a sqlalchemy "Textual SQL" object from the flowmachine SQL string
        textual_sql = text(self.flowmachine_query.get_query())

        # Create sqlalchemy column objects for each of the columns
        # in the output of the flowmachine query
        sqlalchemy_columns = [
            column(colname) for colname in self.flowmachine_query.column_names
        ]

        # Explicitly inform the textual query about the output columns we expect
        # and provide an alias. This allows the generated SQL string to be embedded
        # as a subquery in other queries.
        sqlalchemy_subset_query = textual_sql.columns(*sqlalchemy_columns).alias(
            "subset_query"
        )

        # Actually perform the subsetting (via a join with the subset query)
        res = select(tbl.columns).select_from(
            tbl.join(
                sqlalchemy_subset_query,
                tbl.c.subscriber == sqlalchemy_subset_query.c.subscriber,
            )
        )

        return res


class SubscriberSubsetterForExplicitSubset(SubscriberSubsetterBase):
    """
    Represents a subset given by an explicit list of subscribers.
    """

    is_proper_subset = True

    def __init__(self, subscribers):
        valid_input_types = (list, tuple, np.ndarray, pd.Series)
        if not isinstance(subscribers, valid_input_types):
            raise TypeError(
                f"Invalid input type: {type(subscribers)}. Must be one of: {valid_input_types}"
            )

        self.subscribers = subscribers
        super().__init__()

    def _make_query(self):
        # Return a dummy string representing this subset. This is only needed
        # because SubscriberSubsetterBase currently inherits from Query, but will
        # eventually be removed.
        return "<SubscriberSubsetterForExplicitSubset>"

    def apply_subset_if_needed(self, sql, *, subscriber_identifier):
        """
        Return a modified version of the input SQL query which has the subset applied.

        Parameters
        ----------
        sql : sqlalchemy.sql.ClauseElement
            The SQL query to which the subset should be applied.

        subscriber_identifier : str
            The column in the parent table which contains the subscriber information.

        Returns
        ----------
        sqlalchemy.sql.ClauseElement
        """
        assert isinstance(sql, ClauseElement)
        assert len(sql.froms) == 1
        parent_table = sql.froms[0]
        return sql.where(parent_table.c[subscriber_identifier].in_(self.subscribers))


def make_subscriber_subsetter(subset):
    """
    Return an appropriate subsetter for the given input.

    Parameters
    ----------
    subset : "all" or None or list or tuple or flowmachine.Query or SubscriberSubsetterBase
        This can be one of the following:
          - "all" or None: represents the subset of "all subscribers (i.e., no subsetting at all)
          - list or tuple: represents a subset of an explicit list of subscribers
          - flowmachine.Query: represents a subset given by the result of a flowmachine query
            (where the resulting table must have a "subscriber" column)
        If `subset` is already an instance of SubscriberSubsetterBase then it is returned unchanged.
    """
    if isinstance(subset, SubscriberSubsetterBase):
        return subset
    elif isinstance(subset, Query):
        return SubscriberSubsetterForFlowmachineQuery(subset)
    elif isinstance(subset, (list, tuple, np.ndarray, pd.Series)):
        return SubscriberSubsetterForExplicitSubset(subset)
    elif subset == "all" or subset is None:
        return SubscriberSubsetterForAllSubscribers()
    elif isinstance(subset, str):
        return SubscriberSubsetterForExplicitSubset([subset])
    else:
        raise ValueError(f"Invalid subscriber subset: {subset!r}")
