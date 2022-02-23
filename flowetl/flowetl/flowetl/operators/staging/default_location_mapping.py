# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from airflow.operators.postgres_operator import PostgresOperator


class DefaultLocationMapping(PostgresOperator):
    """
    Creates a table defining a 1-1 mapping of cell tower location to cell tower location.
    Used as a placeholder until/if we implement more sophisticated tower mapping techniques.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(
            *args,
            task_id="DefaultLocationMapping",
            sql="default_location_mapping.sql",
            **kwargs
        )
