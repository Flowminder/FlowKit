# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
from typing import List

from .query import Query
import numbers
from flowmachine.utils import _makesafe


class _SubsetGetter:
    """
    Helper class for pickling/unpickling of dynamic subset classes.
    """

    def __call__(self, parent, col, subset):
        return parent.subset(col, subset)


class _NumSubsetGetter:
    """
    Helper class for pickling/unpickling of dynamic numeric subset classes.
    """

    def __call__(self, parent, col, low, high):
        return parent.numeric_subset(col, low, high)


def subset_factory(parent_class):
    """
    Creates the subset class dynamically, so that it inherits directly
    from whatever calls the subsetting. Thus a subset will inherit the
    methods of the parent class.
    """

    class Subset(parent_class):
        """
        Subsets one of the columns to a specified subset of values

        It may be the case that you wish to subset by another Query
        object, in this case it is better to use the Query join method.

        Parameters
        ----------
        col : str
            Name of the column to subset, e.g. subscriber, cell etc.
        subset : list
            List of values to subset to
        """

        def __init__(self, parent, col, subsetby):
            self.parent = parent
            self.col = col
            self.subsetby = subsetby
            Query.__init__(self)

        @property
        def column_names(self) -> List[str]:
            return self.parent.column_names

        # This voodoo incantation means that if we look for an attribute
        # in this class, and it is not found, we then look in the parent class
        # it is thus a kind of 'object inheritance'.
        def __getattr__(self, name):
            # Don't extend this to hidden variables, such as _df
            # and _len
            if name.startswith("_"):
                raise AttributeError
            return self.parent.__getattribute__(name)

        def _make_query(self):
            try:
                assert not isinstance(self.subsetby, str)
                ss = tuple(self.subsetby)
            except (TypeError, AssertionError):
                ss = (self.subsetby,)
            finally:
                clause = "parent.{col} IN {subset}".format(
                    col=self.col, subset=_makesafe(ss)
                )

            sql = """
                        SELECT
                            *
                        FROM
                            ({parent}) AS parent
                        WHERE
                            {clause}
                       """.format(
                parent=self.parent.get_query(), clause=clause
            )

            return sql

        def __reduce__(self):
            """
            Returns
            -------
            A special object which recreates subsets.
            """
            return _SubsetGetter(), (self.parent, self.col, self.subsetby)

    return Subset


def subset_numbers_factory(parent_class):
    """
    Creates the NumericSubset class dynamically, similar to subset_factory.
    """

    class NumericSubset(parent_class):
        """
        Subsets one of the columns to a specified range of numerical values.

        It may be the case that you wish to subset by another Query
        object, in this case it is better to use the Query join method.

        Parameters
        ----------
        col : str
            Name of the column to subset, should contain numbers
        low : float
            Lower bound of interval to subset on
        high : float
            Upper bound of interval to subset on
        """

        def __init__(self, parent, col, low, high):
            self.parent = parent
            self.col = col
            self.low = low
            self.high = high
            try:
                assert isinstance(self.low, numbers.Real) and isinstance(
                    self.high, numbers.Real
                )
            except AssertionError:
                raise TypeError("Low and/or High not and integer or float")
            Query.__init__(self)

        @property
        def column_names(self) -> List[str]:
            return self.parent.column_names

        def __getattr__(self, name):
            # Don't extend this to hidden variables, such as _df
            # and _len
            if name.startswith("_"):
                raise AttributeError
            return self.parent.__getattribute__(name)

        def _make_query(self):
            low = _makesafe(self.low)
            high = _makesafe(self.high)
            clause = "parent.{col} BETWEEN {low} AND {high}".format(
                col=self.col, low=low, high=high
            )
            sql = """
                        SELECT
                            *
                        FROM
                            ({parent}) AS parent
                        WHERE
                            {clause}
                       """.format(
                parent=self.parent.get_query(), clause=clause
            )

            return sql

        def __reduce__(self):
            """
            Returns
            -------
            A special object which recreates numeric subsets.
            """
            return _NumSubsetGetter(), (self.parent, self.col, self.low, self.high)

    return NumericSubset
