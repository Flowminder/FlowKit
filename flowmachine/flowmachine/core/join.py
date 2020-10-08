# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from typing import List

from .query import Query


class Join(Query):
    """
    Class that results when joining two queries. Don't usually
    call this directly, instead use the join method of query.

    Parameters
    ----------
    left : Query
        Query object to join on.
    right : Query
        Query object to join on.
    on_left : str or list of str
        Name of the column on the left table on which to join, if a list
        will join on the fact that each field is equal
    on_right : str, optional
        Name of the column on the right table on which to join, if not
        specified will default to the same as on_left, if a list must be
        the same length as on_left.
    how : str, default 'inner'
        The method by which to join. Based on the methods provided by
        postgres. These can be:'inner', 'left outer', 'right outer'
        and 'full outer'.
    left_append : str, default ''
        Text with which to append the columns from the left table. Note
        that the column given by 'on_left' will not be appended to.
    right_append : str, default ''
        Text with which to append the columns from the left table.

    Notes
    -----
    This is not a full implementation of the postgres join api. It assumes
    joining by matching columns from table1 and table2. The user can
    construct more complex custom joins by hand.
    """

    join_kinds = ("inner", "full outer", "left", "right", "left outer", "right outer")

    def __init__(
        self,
        left,
        right,
        on_left,
        on_right=None,
        how="inner",
        left_append="",
        right_append="",
    ):
        """"""

        self.left = left
        self.right = right
        if left_append == right_append and not set(left.column_names).isdisjoint(
            right.column_names
        ):
            raise ValueError(
                f"Columns {set(left.column_names).intersection(right.column_names)} are ambiguous. Pass left_append or right_append to disambiguate."
            )

        # Always store the join columns as a list, even if there is only one of them
        if type(on_left) is str:
            self.on_left = [on_left]
        else:
            self.on_left = on_left

        if on_right is None:
            self.on_right = self.on_left
        else:
            if type(on_right) is str:
                self.on_right = [on_right]
            else:
                self.on_right = on_right

        if len(self.on_left) != len(self.on_right):
            raise ValueError("on_left and on_right must be the same length")
        self.join_method = how.upper()
        self.left_append = left_append
        self.right_append = right_append
        if how.lower() not in self.join_kinds:
            raise ValueError(
                f"'{how}' is not a valid join kind. Use of one of {self.join_kinds}"
            )

        super().__init__()

    @property
    def _select_targets(self) -> List[str]:
        """
        Return a list of the SELECT targets for use in `_make_query`

        Returns
        -------
        list
            List of columns to include
        """

        # Get all the column names, qualified with the table name
        all_left = [f"t1.{c}" for c in self.left.column_names]
        all_right = [f"t2.{c}" for c in self.right.column_names]

        # use on_left and on_right locally with qualification of the table
        # alias
        on_left = [f"t1.{l}" for l in self.on_left]
        on_right = [f"t2.{r}" for r in self.on_right]

        # Now pop the focal column name
        for r, l in zip(on_right, on_left):
            all_left.remove(l)
            all_right.remove(r)

        # Get the focal column
        if self.join_method.lower() == "inner":
            focal = on_left
        elif self.join_method.lower() == "full outer":
            focal = [
                f"coalesce({l}, {r}) AS {l.split('.')[-1]}"
                for r, l in zip(on_left, on_right)
            ]
        elif self.join_method.lower() in ("left outer", "left"):
            focal = on_left
        elif self.join_method.lower() in ("right outer", "right"):
            focal = on_right

        # Now add the append if necessary
        all_columns = (
            focal
            + [f"{l} AS {l.split('.')[-1]}{self.left_append}" for l in all_left]
            + [f"{r} AS {r.split('.')[-1]}{self.right_append}" for r in all_right]
        )

        return all_columns

    @property
    def column_names(self) -> List[str]:
        all_cols = self._select_targets
        cols = []
        if "FULL OUTER" != self.join_method:
            # In FULL OUTER, the focal columns get aliased, otherwise
            # they just refer by table.column_name
            cols = [c.split(".").pop() for c in all_cols[: len(self.on_left)]]
            all_cols = all_cols[len(self.on_left) :]
        # Take the RHS of the aliasing
        cols += [col.split(" AS ").pop() for col in all_cols]
        return cols

    def _make_query(self):
        join_clause = " AND ".join(
            f"t1.{l} = t2.{r}" for l, r in zip(self.on_left, self.on_right)
        )

        sql = f"""
        SELECT
            {",".join(self._select_targets)}
        FROM
            ({self.left.get_query()}) AS t1
        {self.join_method} JOIN
            ({self.right.get_query()}) as t2
        ON
            {join_clause}
        """

        return sql


# Give the join method of the Query class the doc string of the Join classes init
# method
Query.join.__doc__ = Join.__init__.__doc__
