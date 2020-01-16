from typing import Type


class WrappingSQLMixin:
    def prepare_template(self) -> None:
        self.sql = self.wrapper_sql.format(sql=self.sql)


def wrapped_sql_operator(*, class_name: str, sql: str) -> Type:
    from flowetl.mixins.table_name_macros_mixin import TableNameMacrosMixin
    from airflow.operators.postgres_operator import PostgresOperator

    return type(
        class_name,
        (TableNameMacrosMixin, WrappingSQLMixin, PostgresOperator),
        dict(wrapper_sql=sql),
    )
