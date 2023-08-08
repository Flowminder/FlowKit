from typing import Callable

from airflow.decorators import task, dag as dag_dec
from airflow.providers.docker.operators.docker import DockerOperator
import os
from datetime import datetime
from docker.types import Mount


def build_flowpyter_task(task_name=None) -> Callable:
    @task.docker(
        image="flowminder/flowpyterlab:api-analyst-latest",
        task_id=task_name,
        mount_tmp_dir=False,
        mounts=[
            Mount(
                source="/home/john/projects/airflow_notebooks/notebooks",
                target="/opt/airflow/notebooks/",
                type="bind",
            ),
        ],
        environment={"FLOWAPI_TOKEN": "{{ var.value.flowapi_token }}"},
        network_mode="container:flowapi",
    )
    # We need to include notebook_name and nb_paras args here because it isn't closing over the context for some
    # reason - could be to do with the wrapper? I'm wondering if functools.update_wrapper will solve this somehow.
    def this_task(
        execution_date=None,
        previous_notebook=None,
        nb_params=None,
        notebook_name=None,
    ):
        if nb_params is None:
            nb_params = {}
        if previous_notebook is not None:
            previous_notebook = previous_notebook[
                0
            ]  # This gets put in a list when passed between tasks
        import papermill as pm

        context_params = {
            "execution_date": execution_date,
            "flowapi_url": "http://localhost:9090",  # TODO: Replace with env var
            "previous_notebook": previous_notebook,
        }
        task_params = context_params | nb_params

        out_path = f"/opt/airflow/notebooks/out/{notebook_name}-{execution_date}.ipynb"
        in_path = f"/opt/airflow/notebooks/{notebook_name}.ipynb"
        pm.execute_notebook(
            in_path,
            out_path,
            parameters=task_params,
            progress_bar=False,
        )
        return out_path

    return this_task


@dag_dec(
    dag_id="flows_report",
    default_args={"retries": 0},
    schedule="0 0 * * *",
    start_date=datetime(2023, 7, 27),
    template_searchpath="/usr/local/airflow/include",
    catchup=False,
)
def flows_report_taskflow():
    run_flows_task = build_flowpyter_task(
        "run_flows",
    )

    flows_report_task = build_flowpyter_task(
        "flows_report",
    )

    flows_nb = (
        run_flows_task(
            notebook_name="run_flows",
            nb_params={
                "aggregation_unit": "admin2",
                "date_ranges": [
                    ["2016-01-01", "2016-01-03"],
                    ["2016-01-03", "2016-01-04"],
                    ["2016-01-04", "2016-01-05"],
                ],
            },
        ),
    )
    flows_report_task(
        previous_notebook=flows_nb,
        notebook_name="flows_report",
        nb_params={
            "aggregation_unit": "admin2",
            "reference_date": "2016-01-04",
        },
    )


flows_report_taskflow()

if __name__ == "__main__":
    flows_report_taskflow()
