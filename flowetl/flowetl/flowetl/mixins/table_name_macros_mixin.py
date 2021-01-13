# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from typing import Dict, Optional

import jinja2


class TableNameMacrosMixin:
    def render_template_fields(
        self, context: Dict, jinja_env: Optional[jinja2.Environment] = None
    ) -> None:

        cdr_type = context["params"]["cdr_type"]
        table_name = f"{cdr_type}_{context.get('ds_nodash')}"

        etl_schema = "etl"
        final_schema = "events"
        parent_table = f"{final_schema}.{cdr_type}"
        extract_table_name = f"extract_{table_name}"
        staging_table_name = f"stg_{table_name}"
        final_table = f"{final_schema}.{table_name}"
        extract_table = f"{etl_schema}.{extract_table_name}"
        staging_table = f"{etl_schema}.{staging_table_name}"

        context = dict(
            parent_table=parent_table,
            table_name=table_name,
            etl_schema=etl_schema,
            final_schema=final_schema,
            extract_table_name=extract_table_name,
            staging_table_name=staging_table_name,
            final_table=final_table,
            extract_table=extract_table,
            staging_table=staging_table,
            cdr_type=cdr_type,
            **context,
        )
        super().render_template_fields(context, jinja_env=jinja_env)
