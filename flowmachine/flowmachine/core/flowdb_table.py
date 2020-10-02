from flowmachine.core.table import Table


class FlowDBTable(Table):
    def __init__(self, *, name, schema, columns):
        if columns is None:
            columns = self.all_columns
        if set(columns).issubset(self.all_columns):
            super().__init__(schema=schema, name=name, columns=columns)
        else:
            raise ValueError(
                f"Columns {columns} must be a subset of {self.all_columns}"
            )
