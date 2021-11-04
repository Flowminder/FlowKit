from typing import List

from flowmachine.core.query import Query


class UniqueSubscribersFromQueries(Query):
    """
    Given a list of queries with a 'subscriber' column, returns a table of unique subscribers.
    """

    def __init__(self, query_list: List[Query]):
        self.query_list = query_list
        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return ["subscribers"]

    def _make_query(self):

        for query in self.query_list:
            union_stack = "\nUNION ALL\n".join(
                f"""
            SELECT subscriber FROM
            ({query.get_query()}) as tbl"""
            )

        sql = f"""
            SELECT subscriber
            FROM (
                {union_stack}
            ) AS unioned
            GROUP BY subscriber
        """
        return sql
