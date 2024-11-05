# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flowmachine.core.query import Query


class QStub(Query):
    """
    A stub representation of a Query object used for version-aware caching.

    This class serves as a lightweight placeholder for cached queries,
    particularly when handling queries from different versions.

    Parameters
    ----------
    deps : list[Query]
        The dependencies of the original query
    qid : str
        The query ID hash of the original query
    flowmachine_version : str
        The flowmachine version used to create this cache record
    """

    def __init__(self, deps: list["Query"], qid: str, flowmachine_version: str) -> None:
        """
        Parameters
        ----------
        deps : list[Query]
            The dependencies of the original query
        qid : str
            The query ID hash of the original query
        """
        self.deps = deps
        self._md5 = qid
        self.flowmachine_version = flowmachine_version
        super().__init__()

    def _make_query(self) -> str:
        """
        Not implemented for stub queries.

        Raises
        ------
        NotImplementedError
            Always, as stub queries cannot generate SQL.
        """
        raise NotImplementedError("Stub queries cannot generate SQL")

    @property
    def column_names(self) -> list[str]:
        """
        Not implemented for stub queries.

        Raises
        ------
        NotImplementedError
            Always, as stub queries cannot generate SQL.
        """
        raise NotImplementedError("Stub queries cannot provide column names.")
