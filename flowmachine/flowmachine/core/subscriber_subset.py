from abc import abstractmethod
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


    def _get_query_attrs_for_dependency_graph(self, analyse=False):
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


class OtherSubset(SubscriberSubsetBase):

    is_proper_subset = True

    def __init__(self, subset):
        self.ORIG_SUBSET_TODO_REMOVE_THIS = subset

    def get_query(self):
        return self.ORIG_SUBSET_TODO_REMOVE_THIS.get_query()

    def _make_query(self):
        return "<OtherSubset>"

    def apply_subset(self, sql):
        raise NotImplementedError()


def make_subscriber_subset(subset):
    if isinstance(subset, SubscriberSubsetBase):
        return subset
    elif subset == "all" or subset is None:
        return AllSubscribers()
    else:
        return OtherSubset(subset)
    # else:
    #     raise ValueError(f"Invalid subscriber subset: {subset!r}")
