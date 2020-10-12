# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Classes to select random samples from queries or tables.
"""
import random
from typing import List, Optional, Dict, Any, Union, Type, Tuple
from abc import ABCMeta, abstractmethod

from .query import Query
from .table import Table


class _RandomGetter:
    """
    Helper class for pickling/unpickling of dynamic random classes.
    (see https://stackoverflow.com/questions/1947904/how-can-i-pickle-a-nested-class-in-python/11493777#11493777)
    """

    def __call__(self, query: Query, sampling_method: str, params: Dict[str, Any]):
        return query.random_sample(sampling_method, **params)


class RandomBase(metaclass=ABCMeta):
    """
    Base class for queries used to obtain a random sample from a table.
    """

    def __init__(
        self,
        query: Query,
        *,
        size: Optional[int] = None,
        fraction: Optional[float] = None,
        estimate_count: bool = False,
    ):
        if size is None and fraction is None:
            raise ValueError(
                f"{self.__class__.__name__}() missing 1 required argument: 'size' or 'fraction'"
            )
        if size is not None and fraction is not None:
            raise ValueError(
                f"{self.__class__.__name__}() expects only 1 argument to be defined: either 'size' or 'fraction'"
            )

        if fraction is not None and (fraction < 0 or fraction > 1):
            raise ValueError(
                f"{self.__class__.__name__}() expects fraction between 0 and 1."
            )

        self.query = query
        self.size = size
        self.fraction = fraction
        self.estimate_count = estimate_count

    @property
    def _sample_params(self) -> Dict[str, Any]:
        """
        Parameters passed when initialising this query.
        """
        return {
            "size": self.size,
            "fraction": self.fraction,
            "estimate_count": self.estimate_count,
        }

    def _count_table_rows(self) -> int:
        """
        Return a count of the number of rows in self.query table, either using
        information contained in the `pg_class` (if self.estimate_rowcount) or
        by performing an actual count in the number of rows.
        """
        rowcount = 0

        if self.estimate_count:
            table = self.query.get_table()
            rowcount = table.estimated_rowcount()

        if not self.estimate_count or rowcount == 0:
            rowcount = len(self.query)

        return rowcount

    @abstractmethod
    def _make_query(self):
        raise NotImplementedError(
            f"Class {self.__class__.__name__} does not implement the _make_query method."
        )

    # Overwrite the table_name method so that it cannot
    # be stored by accident.
    @property
    def table_name(self):
        raise NotImplementedError("Unseeded random samples cannot be stored.")

    # Overwrite to call on parent instead
    @property
    def column_names(self) -> List[str]:
        return self.query.column_names


class RandomSystemRows(RandomBase):
    """
    Gets a random sample from the result of a query, using a PostgreSQL TABLESAMPLE
    clause with the 'system_rows' method.
    This method performs block-level sampling by randomly sampling
    each physical storage page of the underlying relation. This
    sampling method is guaranteed to provide a sample of the specified
    size.

    Parameters
    ----------
    query : str
        A query specifying a table from which a random sample will be drawn.
    size : int, optional
        The number of rows to be selected from the table.
        Exactly one of the 'size' or 'fraction' arguments must be provided.
    fraction : float, optional
        The fraction of rows to be selected from the table.
        Exactly one of the 'size' or 'fraction' arguments must be provided.
    estimate_count : bool, default False
        Whether to estimate the number of rows in the table using
        information contained in the `pg_class` or whether to perform an
        actual count in the number of rows.

    See Also
    --------
    flowmachine.core.random.random_factory

    Notes
    -----
    The 'system_rows' sampling method does not support parent tables which have
    child inheritance.
    The 'system_rows' sampling method does not support supplying a seed for
    reproducible samples, so random samples cannot be stored.
    """

    def __init__(
        self,
        query: Query,
        *,
        size: Optional[int] = None,
        fraction: Optional[float] = None,
        estimate_count: bool = False,
    ):
        # Raise a value error if the query is a table, and has children, as the
        # method relies on it not having children.
        if isinstance(query, Table) and query.has_children():
            raise ValueError(
                "It is not possible to use the 'system_rows' method in tables with inheritance "
                + "as it selects a random sample for each child table and not for the set as a whole."
            )

        super().__init__(
            query=query, size=size, fraction=fraction, estimate_count=estimate_count
        )

    def _make_query(self) -> str:
        # TABLESAMPLE only works on tables, so silently store this query
        self.query.store().result()

        columns = ", ".join(
            [
                "{}.{}".format(self.query.fully_qualified_table_name, c)
                for c in self.query.column_names
            ]
        )

        if self.size is None:
            rowcount = self._count_table_rows()
            size = int(self.fraction * float(rowcount))
        else:
            size = self.size

        sampled_query = f"""
        SELECT {columns} FROM {self.query.fully_qualified_table_name} TABLESAMPLE SYSTEM_ROWS({size})
        """

        return sampled_query


class SeedableRandom(RandomBase, metaclass=ABCMeta):
    """
    Base class for random samples that accept a seed parameter for reproducibility.
    """

    def __init__(
        self,
        query: Query,
        *,
        size: Optional[int] = None,
        fraction: Optional[float] = None,
        estimate_count: bool = False,
        seed: Optional[float] = None,
    ):
        self._seed = seed
        super().__init__(
            query=query, size=size, fraction=fraction, estimate_count=estimate_count
        )

    # Make seed a property to avoid inadvertently changing it.
    @property
    def seed(self) -> Optional[float]:
        return self._seed

    @property
    def _sample_params(self) -> Dict[str, Any]:
        """
        Parameters passed when initialising this query.
        """
        return dict(seed=self.seed, **super()._sample_params)

    # Overwrite the table_name method so that it cannot
    # be stored by accident.
    @property
    def table_name(self) -> str:
        if self.seed is None:
            raise NotImplementedError("Unseeded random samples cannot be stored.")
        return f"x{self.query_id}"


class RandomTablesample(SeedableRandom):
    """
    Gets a random sample from the result of a query, using a PostgreSQL TABLESAMPLE
    clause with one of the following sampling methods:
    'system': performs block-level sampling by randomly sampling each
        physical storage page for the underlying relation. This
        sampling method is not guaranteed to generate a sample of the
        specified size, but an approximation. This method may not
        produce a sample at all, so it might be worth running it again
        if it returns an empty dataframe.
    'bernoulli': samples directly on each row of the underlying
        relation. This sampling method is slower and is not guaranteed to
        generate a sample of the specified size, but an approximation.
    The choice of method is determined from the _sampling_method attribute.

    Parameters
    ----------
    query : str
        A query specifying a table from which a random sample will be drawn.
    size : int, optional
        The number of rows to be selected from the table.
        Exactly one of the 'size' or 'fraction' arguments must be provided.
    fraction : float, optional
        The fraction of rows to be selected from the table.
        Exactly one of the 'size' or 'fraction' arguments must be provided.
    estimate_count : bool, default False
        Whether to estimate the number of rows in the table using
        information contained in the `pg_class` or whether to perform an
        actual count in the number of rows.
    seed : float, optional
        Optionally provide a seed for repeatable random samples.

    See Also
    --------
    flowmachine.core.random.random_factory

    Notes
    -----
    Random samples may only be stored if a seed is supplied.
    """

    _sampling_method = None

    def __init__(
        self,
        query: Query,
        *,
        size: Optional[int] = None,
        fraction: Optional[float] = None,
        estimate_count: bool = False,
        seed: Optional[float] = None,
    ):
        valid_methods = ["system", "bernoulli"]
        if self._sampling_method not in valid_methods:
            raise ValueError(
                "RandomTablesample() expects a valid sampling method from any of these: "
                + ", ".join(valid_methods)
            )

        super().__init__(
            query=query,
            size=size,
            fraction=fraction,
            estimate_count=estimate_count,
            seed=seed,
        )

    def _make_query(self) -> str:
        # TABLESAMPLE only works on tables, so silently store this query
        self.query.store().result()

        columns = ", ".join(
            [
                "{}.{}".format(self.query.fully_qualified_table_name, c)
                for c in self.query.column_names
            ]
        )

        if self.fraction is None:
            rowcount = self._count_table_rows()
            percent = 100 * self.size / float(rowcount)
        else:
            percent = self.fraction * 100

        repeatable_statement = (
            f"REPEATABLE({self.seed})" if self.seed is not None else ""
        )

        if self.size is not None:
            percent_buffer = min(percent + 10, 100)
            sampled_query = f"""
            SELECT {columns} FROM {self.query.fully_qualified_table_name}
            TABLESAMPLE {self._sampling_method.upper()}({percent_buffer}) {repeatable_statement} LIMIT {self.size}
            """
        else:
            sampled_query = f"""
            SELECT {columns} FROM {self.query.fully_qualified_table_name}
            TABLESAMPLE {self._sampling_method.upper()}({percent}) {repeatable_statement}
            """

        return sampled_query


class RandomIDs(SeedableRandom):
    """
    Gets a random sample from the result of a query, using the 'random_ids' sampling method.
    This method samples rows by randomly sampling the row number.

    Parameters
    ----------
    query : str
        A query specifying a table from which a random sample will be drawn.
    size : int, optional
        The number of rows to be selected from the table.
        Exactly one of the 'size' or 'fraction' arguments must be provided.
    fraction : float, optional
        The fraction of rows to be selected from the table.
        Exactly one of the 'size' or 'fraction' arguments must be provided.
    estimate_count : bool, default False
        Whether to estimate the number of rows in the table using
        information contained in the `pg_class` or whether to perform an
        actual count in the number of rows.
    seed : float, optional
        Optionally provide a seed for repeatable random samples.
        For the 'random_ids' method, seed must be between -/+1.

    See Also
    --------
    flowmachine.core.random.random_factory

    Notes
    -----
    Random samples may only be stored if a seed is supplied.
    """

    def __init__(
        self,
        query: Query,
        *,
        size: Optional[int] = None,
        fraction: Optional[float] = None,
        estimate_count: bool = False,
        seed: Optional[float] = None,
    ):
        if seed is not None and (seed > 1 or seed < -1):
            raise ValueError("Seed must be between -1 and 1 for random_ids method.")

        super().__init__(
            query=query,
            size=size,
            fraction=fraction,
            estimate_count=estimate_count,
            seed=seed,
        )

    def _make_query(self) -> str:
        # TABLESAMPLE only works on tables, so silently store this query
        # Note: The "random_ids" method doesn't use TABLESAMPLE, but we still
        # store the query before sampling for consistency with the other
        # sampling methods.
        self.query.store().result()

        columns = ",".join(self.query.column_names)

        rowcount = self._count_table_rows()

        if self.size is None:
            size = int(self.fraction * float(rowcount))
        else:
            size = self.size

        # Set the seed used to a random one if none is provided
        seed = self.seed if self.seed is not None else random.random()

        sampled_query = f"""
        SELECT {columns} FROM (
            (SELECT id as rid FROM random_ints({seed}, {size}, {rowcount})) r
            LEFT JOIN
            (SELECT *, row_number() OVER () as rid FROM {self.query.fully_qualified_table_name}) s
            USING (rid)
        ) o
        """

        return sampled_query


def random_factory(parent_class: Type[Query], sampling_method: str = "random_ids"):
    """
    Dynamically creates a random class as a descendant of parent_class.
    The resulting object will query the underlying object for attributes,
    and methods.

    Parameters
    ----------
    parent_class : class derived from flowmachine.core.Query
        Class from which to derive random class
    sampling_method : str, default 'random_ids'
        One of 'system_rows', 'system', 'bernoulli', 'random_ids'.
        Specifies the method used to select the random sample.
        'system_rows': performs block-level sampling by randomly sampling
            each physical storage page of the underlying relation. This
            sampling method is guaranteed to provide a sample of the specified
            size. This method does not support parent tables which have child
            inheritance, and is not reproducible.
        'system': performs block-level sampling by randomly sampling each
            physical storage page for the underlying relation. This
            sampling method is not guaranteed to generate a sample of the
            specified size, but an approximation. This method may not
            produce a sample at all, so it might be worth running it again
            if it returns an empty dataframe.
        'bernoulli': samples directly on each row of the underlying
            relation. This sampling method is slower and is not guaranteed to
            generate a sample of the specified size, but an approximation.
        'random_ids': samples rows by randomly sampling the row number.

    Returns
    -------
    class
        A class which gets a random sample from the result of a query.

    Examples
    --------
        >>> query = UniqueSubscribers("2016-01-01", "2016-01-31")
        >>> Random = random_factory(query.__class__)
        >>> Random(query=query, size=10).get_dataframe()

                        msisdn
        0  AgvE8pa3Bvqezmo6
        1  3XKdxqvyNxO2vLD1
        2  5Kgwy8Gp6DlN3Eq9
        3  L4V537alj321eWz6
        4  GJP3DWdGyb4QBnyo
        5  DAlqeZENbeOn2vBw
        6  By4j6PKdB4NGMpxr
        7  mkqQ4NPBPQLapbeg
        8  YNv2EgDJxxAoy0Gr
        9  2vmOlAENnxpPM1xX

        >>> query = VersionedInfrastructure("2016-01-01")
        >>> Random = random_factory(query.__class__)
        >>> Random(query=query, size=10).get_dataframe()

                id  version
        0  o9yyxY        0
        1  B8OaG5        0
        2  DbWg4K        0
        3  0xqNDj        0
        4  pqg7ZE        0
        5  nWM8R3        0
        6  LVnDQL        0
        7  pdVVV4        0
        8  wzrXjw        0
        9  RZgwVz        0

        # The default method 'system_rows' does not support parent tables which have child inheritance
        # as is the case with 'events.calls', so we choose another method here.
        >>> Random = random_factory(flowmachine.core.Query, sampling_method='bernoulli')
        >>> Random(query=Table('events.calls', columns=['id', 'duration']), size=10).get_dataframe()
                                id  duration
        0  mQjOy-5eVrm-Ll5eE-P4V27     422.0
        1  mQjOy-5eVrm-Ll5eE-P4V27     422.0
        2  0r4KG-Rb4Lm-VK1bB-LZQxg     762.0
        3  BDXMV-yb8Kl-zkmav-AZEJ2     318.0
        4  vm9gW-4QbYm-OrKbz-qM5Yx    1407.0
        5  WYxk8-mepk9-W3pdM-yJNjQ    1062.0
        6  mQjOy-5eVn3-wK5eE-P4V27    1033.0
        7  M7Vl4-zbqom-oPDep-rOZqE     879.0
        8  58DKg-l9av9-NE8eG-1vzAp    3129.0
        9  m9gW4-QbY62-WLYdz-qM5Yx    1117.0
    """
    random_classes = {
        "system_rows": RandomSystemRows,
        "system": RandomTablesample,
        "bernoulli": RandomTablesample,
        "random_ids": RandomIDs,
    }

    try:
        random_class = random_classes[sampling_method]
    except KeyError:
        raise ValueError(
            "random_factory expects a valid sampling method from any of these: "
            + ", ".join(random_classes.keys())
        )

    class Random(random_class, parent_class):
        __doc__ = random_class.__doc__

        # We define _sampling_method here so that __reduce__ can pass this to
        # _RandomGetter for pickling/unpickling, and also so that it can be
        # used within the RandomTablesample class without having to pass
        # sampling_method as an init parameter
        _sampling_method = sampling_method

        def __init__(self, query: Query, **params):
            super().__init__(query=query, **params)
            Query.__init__(self)

        # This voodoo incantation means that if we look for an attibute
        # in this class, and it is not found, we then look in the parent class
        # it is thus a kind of 'object inheritance'.
        def __getattr__(self, name):
            # Don't extend this to hidden variables, such as _df
            # and _len
            if name.startswith("_"):
                raise AttributeError
            return self.query.__getattribute__(name)

        def __reduce__(self) -> Tuple[_RandomGetter, Tuple[Query, str, Dict[str, Any]]]:
            """
            Returns
            -------
            A special object which recreates random samples.
            """
            return (
                _RandomGetter(),
                (self.query, self._sampling_method, self._sample_params),
            )

    return Random
