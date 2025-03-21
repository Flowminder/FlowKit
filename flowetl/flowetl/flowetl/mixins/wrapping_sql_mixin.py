# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from typing import Type

from jinja2 import TemplateNotFound


class WrappingSQLMixin:
    def prepare_template(self) -> None:
        try:
            # See if the sql to be wrapped lives in a file
            jinja_env = self.dag.get_template_env()
            sql = jinja_env.loader.get_source(jinja_env, self.sql)[0]
        except TemplateNotFound:
            sql = self.sql
        self.sql = self.wrapper_sql.format(sql=sql)


def wrapped_sql_operator(*, class_name: str, sql: str) -> Type:
    """
    Creates a new operator type which wraps user supplied SQL in
    a fixed sql template.

    Parameters
    ----------
    class_name : str
        Class name for the operator
    sql : str
        SQL string to use as wrapper

    Returns
    -------
    Type

    """
    from flowetl.mixins.table_name_macros_mixin import TableNameMacrosMixin
    from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator

    return type(
        class_name,
        (TableNameMacrosMixin, WrappingSQLMixin, SQLExecuteQueryOperator),
        dict(
            wrapper_sql=sql,
        ),
    )
