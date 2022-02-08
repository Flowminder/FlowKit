from pathlib import Path

func_template = """
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from airflow.operators.postgres_operator import PostgresOperator

class {op_name}(PostgresOperator):
    def __init__(self,*args, **kwargs):
        super().__init__(*args,
        task_id = "{op_name}",
        sql="{sql_file}",
        **kwargs
)
"""

for query in Path("operators/sql").iterdir():
    if query.suffix == ".sql":
        query_name = query.stem
        op_name = "".join(map(lambda x: x.capitalize(), query_name.split("_")))
        with open(Path("operators") / (query_name + ".py"), "w") as op_out:
            op_out.write(func_template.format(op_name=op_name, sql_file=query))
