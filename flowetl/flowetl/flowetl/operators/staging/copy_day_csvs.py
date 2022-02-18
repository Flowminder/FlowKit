from airflow.operators.bash_operator import BashOperator


class CopyDayCsvs(BashOperator):
    def __init__(self, *args, **kwargs):
        task_id = "copy_day_csvs"
        bash_command = (
            "docker cp {{params.flowetl_csv_dir}} flowdb:{{params.flowdb_csv_dir}}"
        )
        super().__init__(*args, task_id=task_id, bash_command=bash_command, **kwargs)
