# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from abc import abstractmethod
from sqlalchemy.sql import ClauseElement, select, text, column
from .query import Query


class SubscriberSubsetBase(Query):
    """
    Base class for the different types of subscriber subsets.
    """

    @property
    @abstractmethod
    def is_proper_subset(self):
        raise NotImplementedError(
            f"Class {self.__class__.__name__} does not implement 'is_proper_subset'"
        )

    @abstractmethod
    def apply_subset(self, sql, *, subscriber_identifier):
        raise NotImplementedError(
            f"Class {self.__class__.__name__} does not implement 'apply_subset'"
        )

    def _get_query_attrs_for_dependency_graph(self, analyse=False):
        # This is a stub implementation of this internal method.
        # It is needed because SubscriberSubsetBase currently
        # inherits from flowmachine.Query, so we implement just
        # enough to ensure Query.dependency_graph() doesn't break.
        attrs = {}
        attrs["name"] = self.__class__.__name__
        attrs["stored"] = "N/A"
        attrs["cost"] = "N/A"
        attrs["runtime"] = "N/A"
        return attrs


class AllSubscribers(SubscriberSubsetBase):
    """
    Represents the subset of all subscribers (i.e., no subsetting at all).
    """

    is_proper_subset = False

    def _make_query(self):
        # Return a dummy string representing this subset. This is only needed
        # because SubscriberSubsetBase currently inherits from Query, but will
        # eventually be removed.
        return "<AllSubscribers>"

    def apply_subset(self, sql, *, subscriber_identifier=None):
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


class SubsetFromFlowmachineQuery(SubscriberSubsetBase):
    """
    Represents a subset given by a flowmachine query.
    """

    is_proper_subset = True

    def __init__(self, flowmachine_query):
        assert isinstance(flowmachine_query, Query)

        self.subset_query = text(flowmachine_query.get_query()).columns(
            column("subscriber")
        )

    def _make_query(self):
        # Return a dummy string representing this subset. This is only needed
        # because SubscriberSubsetBase currently inherits from Query, but will
        # eventually be removed.
        return "<SubsetFromFlowmachineQuery>"

    def apply_subset(self, sql, *, subscriber_identifier=None):
        """
        Return a modified version of the input SQL query which has the subset applied.

        Parameters
        ----------
        sql : sqlalchemy.sql.ClauseElement
            The SQL query to which the subset should be applied.

        subscriber_identifier : str
            This argument is ignored for subsets of type 'SubsetFromFlowmachineQuery'.

        Returns
        ----------
        sqlalchemy.sql.ClauseElement
        """
        assert isinstance(sql, ClauseElement)

        tbl = sql.alias("tbl")

        try:
            subset = self.subset_query.distinct().alias("subset")
        except AttributeError:
            # This can happen if `self.subset_query` is a textual query
            subset = self.subset_query.alias("subset")

        res = select(tbl.columns).select_from(
            tbl.join(subset, tbl.c.subscriber == subset.c.subscriber)
        )

        return res


class ExplicitSubset(SubscriberSubsetBase):
    """
    Represents a subset given by an explicit list of subscribers.
    """

    is_proper_subset = True

    def __init__(self, subscribers):
        assert isinstance(subscribers, (list, tuple))
        self.subscribers = subscribers

    def _make_query(self):
        # Return a dummy string representing this subset. This is only needed
        # because SubscriberSubsetBase currently inherits from Query, but will
        # eventually be removed.
        return "<ExplicitSubset>"

    def apply_subset(self, sql, *, subscriber_identifier):
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


def make_subscriber_subset(subset):
    """
    Return an instance of an appropriate subclass of SubscriberSubsetBase representing the given input.

    Parameters
    ----------
    subset : "all" or None or list or tuple or flowmachine.Query or SubscriberSubsetBase
        This can be one of the following:
          - "all" or None: represents the subset of "all subscribers (i.e., no subsetting at all)
          - list or tuple: represents a subset of an explicit list of subscribers
          - flowmachine.Query: represents a subset given by the result of a flowmachine query
            (where the resulting table must have a "subscriber" column)
        If `subset` is already an instance of SubscriberSubsetBase then it is returned unchanged.
    """
    if isinstance(subset, SubscriberSubsetBase):
        return subset
    elif subset == "all" or subset is None:
        return AllSubscribers()
    elif isinstance(subset, str):
        return ExplicitSubset([subset])
    elif isinstance(subset, (list, tuple)):
        return ExplicitSubset(subset)
    elif isinstance(subset, Query):
        return SubsetFromFlowmachineQuery(subset)
    else:
        raise ValueError(f"Invalid subscriber subset: {subset!r}")
