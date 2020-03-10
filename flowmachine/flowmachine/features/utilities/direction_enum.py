# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from enum import Enum
from typing import Optional, List


class Direction(str, Enum):
    IN = "in"
    OUT = "out"
    BOTH = "both"

    @property
    def required_columns(self) -> List[str]:
        """
        The list of columns required for this direction type.

        Returns
        -------
        list of str
            The columns which must be included

        """
        if self == Direction.BOTH:
            return []
        else:
            return ["outgoing"]

    def get_filter_clause(self, prefix: Optional[str] = None) -> str:
        """
        Get the sql filter clause for this enum member.

        Parameters
        ----------
        prefix : str or None
            If given, the table to prefix the outgoing column on

        Returns
        -------
        str
            The filter clause

        Examples
        --------
        >>> Direction.IN.get_filter_clause()
        "WHERE NOT outgoing"
        >>> Direction.OUT.get_filter_clause(prefix="tablename")
        "WHERE tablename.outgoing"

        """
        if self == Direction.BOTH:
            return ""
        else:
            return f"{'NOT' if self == Direction.IN else ''} {'' if prefix is None else f'{prefix}.'}outgoing"
