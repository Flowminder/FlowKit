from airflow.operators.postgres_operator import PostgresOperator
from flowetl.operators.staging.event_columns import event_column_mappings


def create_mount_event_operator(*, event_type: str):
    """
    A class factory function that produces an Airflow operator for mounting a foreign table containing data of
    event_type.
    Parameters
    ----------
    event_type:str
        One of call,sms,location,mds,topup.
    """
    if event_type not in event_column_mappings.keys():
        raise KeyError(f"{event_type} must be one of {event_column_mappings.keys()}")

    columns = ",\n".join(
        f"{col_name} {col_dtype}"
        for col_name, col_dtype in event_column_mappings[event_type].items()
    )

    sql = f"""
    BEGIN;
    CREATE SERVER IF NOT EXISTS csv_fdw FOREIGN DATA WRAPPER file_fdw;

    DROP FOREIGN TABLE IF EXISTS {event_type}_table_{{{{ds_nodash}}}};
    CREATE FOREIGN TABLE {event_type}_table_{{{{ds_nodash}}}}(
        {columns}
    ) SERVER csv_fdw
    OPTIONS (filename '{{{{params.flowdb_csv_dir}}}}/{{{{ds_nodash}}}}_{event_type}.csv', format 'csv', header 'TRUE');

    COMMIT;
    """

    class MountEventTable(PostgresOperator):
        """Mounts an event table with all of it's columns to Postgres."""

        def __init__(self, *args, **kwargs):
            super().__init__(
                *args, task_id=f"Mount{event_type.capitalize()}", sql=sql, **kwargs
            )

    return MountEventTable
