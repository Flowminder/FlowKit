# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from typing import Dict, Optional

from airflow.operators.postgres_operator import PostgresOperator
from flowetl.mixins.table_name_macros_mixin import TableNameMacrosMixin


class CreateForeignStagingTableOperator(TableNameMacrosMixin, PostgresOperator):
    def __init__(
        self,
        *,
        filename: str,
        fields: Dict[str, str],
        program: Optional[str] = None,
        header: bool = True,
        delimiter: str = ",",
        quote: str = '"',
        escape: str = '"',
        null: str = "",
        encoding: Optional[str] = None,
        **kwargs,
    ) -> None:
        # Using an f-string here because filename needs to be templated, which is won't be if it is a param
        sql = f"""
            DROP FOREIGN TABLE IF EXISTS {{{{ staging_table }}}};
            CREATE FOREIGN TABLE {{{{ staging_table }}}} (
            {{{{ params.fields }}}}
            ) SERVER csv_fdw
          OPTIONS ({{% if params.program is defined %}}program {{% else %}}filename {{% endif %}} '{{% if params.program is defined %}}{{{{ params.program }}}} {{% endif %}}{filename}',
                   format 'csv',
                   delimiter '{{{{ params.delimiter }}}}',
                   header '{{{{ params.header }}}}', 
                   null '{{{{ params.null }}}}', 
                   quote '{{{{ params.quote }}}}', 
                   escape '{{{{ params.escape }}}}'
                   {{% if params.encoding is defined %}}, {{{{ params.encoding }}}} {{% endif %}} 
                   );
                """
        fields_string = ",\n\t".join(
            f"{field_name} {field_type.upper()}"
            for field_name, field_type in fields.items()
        )
        params = kwargs.setdefault("params", {})
        params["fields"] = fields_string
        if program is not None:
            params["program"] = program
        if encoding is not None:
            params["encoding"] = encoding
        params["header"] = "TRUE" if header else "FALSE"
        params["null"] = null
        params["quote"] = quote
        params["escape"] = escape
        params["delimiter"] = delimiter
        super().__init__(sql=sql, **kwargs)
