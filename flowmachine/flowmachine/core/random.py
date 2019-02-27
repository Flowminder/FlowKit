# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Classes to select random samples from the database. The random samples could be
random events, msisdns, sites or a given geographical boundary. All the samples
can be subsetted by time.
"""
import random
from typing import List

from .query import Query
from .table import Table


class RandomBase:
    """
    Base class containing method to construct a query used to obtain a random sample from a table.
    """

    def _inheritance_check(self):
        """
        Raise a value error if the query is a table, and has children, and the method relies
         on it not having children.
        """
        if (self.method != "system_rows") or (not isinstance(self.query, Table)):
            return
        if self.query.has_children():
            raise ValueError(
                "It is not possible to use the 'system_rows' method in tables with inheritance "
                + "as it selects a random sample for each child table and not for the set as a whole."
            )

    def _make_query(self):

        # def _make_query(self, columns, table, size=None,
        # fraction=None, method='system_rows', estimate_count=True):

        # TABLESAMPLE only works on tables, so silently store this query
        self.query.store().result()
        table = Table(self.table)

        columns = ", ".join(
            ["{}.{}".format(self.table, c) for c in self.query.column_names]
        )

        size = self.size
        fraction = self.fraction

        if (size and self.method != "system_rows") or (
            fraction and self.method in ["system_rows", "random_ids"]
        ):

            ct = 0

            if self.estimate_count:
                ct = table.estimated_rowcount()

            if not self.estimate_count or ct == 0:
                ct = len(self.query)

            if size is not None:
                fraction = size / float(ct)
            elif fraction is not None:
                size = int(fraction * float(ct))

        if fraction is not None:
            fraction *= 100

        # Set the seed used to a random one if none is provided
        seed = self.seed if self.seed is not None else random.random()

        if self.method == "random_ids":

            query = """
            SELECT {cn} FROM (
                (SELECT * FROM random_ints({seed}, {ct}, {size_buffer})) r
                LEFT JOIN
                (SELECT *, row_number() OVER () as rid FROM {sc}.{tn}) s
                ON r.id=s.rid
            ) o
            LIMIT {size}
            """.format(
                cn=",".join(self.query.column_names),
                sc=table.schema,
                tn=table.name,
                ct=ct,
                size_buffer=int(size * 1.1),
                size=size,
                seed=seed,
            )
        else:
            if size is not None:
                if self.method == "system_rows":
                    fraction_buffer = size
                else:
                    fraction_buffer = min(fraction + 10, 100)
                query = """
                SELECT {cn} FROM {sc}.{tn} TABLESAMPLE {method}({fraction_buffer}) REPEATABLE({seed}) LIMIT {size}
                """.format(
                    cn=columns,
                    sc=table.schema,
                    tn=table.name,
                    fraction_buffer=fraction_buffer,
                    size=size,
                    method=self.method.upper(),
                    seed=seed,
                )
                if self.method == "system_rows":
                    query = """
                                    SELECT {cn} FROM {sc}.{tn} TABLESAMPLE {method}({fraction_buffer}) LIMIT {size}
                                    """.format(
                        cn=columns,
                        sc=table.schema,
                        tn=table.name,
                        fraction_buffer=fraction_buffer,
                        size=size,
                        method=self.method.upper(),
                    )
            else:
                query = """
                SELECT {cn} FROM {sc}.{tn} TABLESAMPLE {method}({fraction}) REPEATABLE({seed})
                """.format(
                    cn=columns,
                    sc=table.schema,
                    tn=table.name,
                    fraction=fraction,
                    method=self.method.upper(),
                    seed=seed,
                )
                if self.method == "system_rows":
                    query = """
                                    SELECT {cn} FROM {sc}.{tn} TABLESAMPLE {method}({fraction})
                                    """.format(
                        cn=columns,
                        sc=table.schema,
                        tn=table.name,
                        fraction=fraction,
                        method=self.method.upper(),
                    )

        return query


def random_factory(parent_class):
    """
    Dynamically creates a random class as a descendant of parent_class.
    The resulting object will query the underlying object for attributes,
    and methods.
    """

    class Random(RandomBase, parent_class):
        """
        Gets a random sample from the database according to the specification.

        Parameters
        ----------
        variable : str
            Either 'msisdn' or 'sites'. The class will select a random sample
            of msisdn or sites. If this argument is set, it is not possible to
            set 'columns' and/or 'query'. The argument 'table' then refers to
            any table in the 'events' schema and only has implications when
            'variable' is equal to 'msisdn'.
        columns : str or list
            The columns from the table to be selected. If this argument is set,
            it is not possible to set 'variable' and/or 'query'.
        table : str
            Schema qualified name of the table which the analysis is based
            upon. If 'ALL' it will use all tables that contain location data,
            specified in flowmachine.yml. If this argument is set, it is not possible
            to set 'query'. If 'variable' is set, then 'table' should refer to
            a table in the 'events' schema.
        query : str
            A query specifying a table from which a random sample will be drawn
            from. If this argument is set, it is not possible to set
            'variable' and/or 'table'.
        size : int
            The size of the random sample.
        fraction : int
            The fraction of rows to be selected from the table.
        method : str, default 'system_rows'
            Either 'system_rows', 'system', 'bernouilli', 'random_ids'.
            Specifies the method used to select the random sample.
            'system_rows': performs block-level sampling by randomly sampling
                each physical storage page of the underlying relation. This
                sampling method is guaranteed to provide a sample of the specified
                size
            'system': performs block-level sampling by randomly sampling each
                physical storage page for the underlying relation. This
                sampling method is not guaranteed to generate a sample of the
                specified size, but an approximation. This method may not
                produce a sample at all, so it might be worth running it again
                if it returns an empty dataframe.
            'bernoulli': samples directly on each row of the underlying
                relation. This sampling method is slower and is not guaranteed to
                generate a sample of the specified size, but an approximation
            'random_ids': Assumes that the table contains a column named 'id'
                with random numbers from 1 to the total number of rows in the
                table. This method samples the ids from this table.
        estimate_count : bool, default True
            Whether to estimate the number of rows in the table using
            information contained in the `pg_class` or whether to perform an
            actual count in the number of rows.

        Examples
        --------
            >>> Random(query=UniqueSubscribers("2016-01-01", "2016-01-31"), size=10).get_dataframe()

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

            >>> Random(VersionedInfrastructure("2016-01-01"), size=10).get_dataframe()

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
            >>> Random(query=Table('events.calls', columns=['id', 'duration'), size=10, method='bernoulli').get_dataframe()
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

        def __init__(
            self,
            query=None,
            size=None,
            fraction=None,
            method="system_rows",
            estimate_count=True,
            seed=None,
        ):
            """

            """

            self.query = query
            self.table = self.query.fully_qualified_table_name
            self.size = size
            self.fraction = fraction
            self.method = method
            self.estimate_count = estimate_count
            self.seed = seed

            if self.size is None and self.fraction is None:
                raise ValueError(
                    "Random() missing 1 required argument: 'size' or 'fraction'"
                )
            if self.size is not None and self.fraction is not None:
                raise ValueError(
                    "Random() expects only 1 argument to be defined: either 'size' or 'fraction'"
                )

            valid_methods = ["system_rows", "system", "bernoulli", "random_ids"]

            if self.method not in valid_methods:
                raise ValueError(
                    "Random() expects a valid method from any of those: "
                    + ", ".join(valid_methods)
                )

            if self.fraction and self.fraction > 1:
                raise ValueError("Random() expects fraction between 0 and 1.")
            self._inheritance_check()
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

        # Overwrite the table_name method so that it cannot
        # be stored by accident.
        @property
        def table_name(self):
            if self.seed is None or self.method == "system_rows":
                raise NotImplementedError
            else:
                return f"x{self.md5}"

        # Overwrite to call on parent instead
        @property
        def column_names(self) -> List[str]:
            return self.query.column_names

    return Random
