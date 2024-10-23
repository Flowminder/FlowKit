# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from abc import ABCMeta
from typing import Optional

from flowmachine.core.table import Table


class FlowDBTable(Table, metaclass=ABCMeta):
    """
    Abstract base class for fixed tables that exist in FlowDB.

    Parameters
    ----------
    name : str
    schema : str
    columns : list of str
    """

    def __init__(self, *, name: str, schema:str , columns:Optional[list[str]]) -> None:
        if columns is None:
            columns = self.all_columns
        if set(columns).issubset(self.all_columns):
            super().__init__(schema=schema, name=name, columns=columns)
        else:
            raise ValueError(
                f"Columns {columns} must be a subset of {self.all_columns}"
            )

    @property
    def all_columns(self):
        raise NotImplementedError
