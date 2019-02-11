from abc import ABCMeta, abstractmethod


class SubscriberSubsetBase(metaclass=ABCMeta):
    """
    Base class for the different types of subscriber subsets.
    """

    @property
    @abstractmethod
    def is_proper_subset(self):
        raise NotImplementedError(f"Class {self.__class__.__name__} does not implement 'is_proper_subset'")

    @property
    @abstractmethod
    def md5(self):
        raise NotImplementedError(f"Class {self.__class__.__name__} does not implement 'md5'")


class AllSubscribers(SubscriberSubsetBase):

    is_proper_subset = False

    @property
    def md5(self):
        return "<AllSubscribers>"


class OtherSubset(SubscriberSubsetBase):

    is_proper_subset = True

    def __init__(self, subset):
        self.ORIG_SUBSET_TODO_REMOVE_THIS = subset

    def get_query(self):
        return self.ORIG_SUBSET_TODO_REMOVE_THIS.get_query()

    @property
    def md5(self):
        return "<OtherSubset>"


def make_subscriber_subset(subset):
    if isinstance(subset, SubscriberSubsetBase):
        return subset
    elif subset == "all" or subset is None:
        return AllSubscribers()
    else:
        return OtherSubset(subset)
    # else:
    #     raise ValueError(f"Invalid subscriber subset: {subset!r}")
