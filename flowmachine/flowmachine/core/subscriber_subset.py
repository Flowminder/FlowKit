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
    def apply_subset(self, sql):
        raise NotImplementedError(
            f"Class {self.__class__.__name__} does not implement 'apply_subset'"
        )

    @abstractmethod
    def apply_subset_sqlalchemy(self, sql, *, PARENT_SUBSCRIBER_IDENTIFIER):
        raise NotImplementedError(
            f"Class {self.__class__.__name__} does not implement 'apply_subset_sqlalchemy'"
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

    is_proper_subset = False

    def _make_query(self):
        return "<AllSubscribers>"

    def apply_subset(self, sql):
        return sql

    def apply_subset_sqlalchemy(self, sql, *, PARENT_SUBSCRIBER_IDENTIFIER):
        return sql


class SubsetFromFlowmachineQuery(SubscriberSubsetBase):

    is_proper_subset = True

    def __init__(self, flowmachine_query):
        self.ORIG_SUBSET_TODO_REMOVE_THIS = flowmachine_query
        self.subset_query = text(flowmachine_query.get_query()).columns(column('subscriber'))

    def _make_query(self):
        return "<SubsetFromFlowmachineQuery>"

    def apply_subset(self, sql):
        raise NotImplementedError()

    def apply_subset_sqlalchemy(self, sql, *, PARENT_SUBSCRIBER_IDENTIFIER=None):
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

    is_proper_subset = True

    def __init__(self, subscribers):
        self.subscribers = subscribers
        self.ORIG_SUBSET_TODO_REMOVE_THIS = subscribers

    def _make_query(self):
        return "<ExplicitSubset>"

    def apply_subset(self, sql):
        raise NotImplementedError()

    def apply_subset_sqlalchemy(self, sql, *, PARENT_SUBSCRIBER_IDENTIFIER):
        assert isinstance(sql, ClauseElement)
        assert len(sql.froms) == 1
        parent_table = sql.froms[0]
        return sql.where(parent_table.c[PARENT_SUBSCRIBER_IDENTIFIER].in_(self.subscribers))


def make_subscriber_subset(subset):
    if isinstance(subset, SubscriberSubsetBase):
        return subset
    elif subset == "all" or subset is None:
        return AllSubscribers()
    elif isinstance(subset, (list, tuple)):
        return ExplicitSubset(subset)
    elif isinstance(subset, Query):
        return SubsetFromFlowmachineQuery(subset)
    else:
        raise ValueError(f"Invalid subscriber subset: {subset!r}")
