from airflow.decorators import task, dag as dag_dec
import os
from datetime import datetime
from docker.types import Mount


@dag_dec(
    dag_id="dockerised_papermill",
    default_args={"retries": 0},
    schedule="0 0 * * *",
    start_date=datetime(2023, 7, 27),
    template_searchpath="/usr/local/airflow/include",
    catchup=False,
)
def docker_taskflow():
    @task.docker(
        image="flowminder/flowpyterlab:api-analyst-latest",
        mount_tmp_dir=False,
        mounts=[
            Mount(
                source="/home/john/projects/airflow_notebooks/notebooks",
                target="/opt/airflow/notebooks/",
                type="bind",
            )
        ],
    )
    def run_notebook(execution_date=None):
        import papermill as pm

        in_path = "/opt/airflow/notebooks/test_nb.ipynb"
        out_path = f"/opt/airflow/notebooks/out/out-docker-{execution_date}.ipynb"
        pm.execute_notebook(
            in_path,
            out_path,
            parameters={"execution_date": execution_date},
            progress_bar=False,
            report_mode=True,
        )
        return out_path

    @task.docker(
        image="flowminder/flowpyterlab:api-analyst-latest",
        mount_tmp_dir=False,
        mounts=[
            Mount(
                source="/home/john/projects/airflow_notebooks/notebooks",
                target="/opt/airflow/notebooks/",
                type="bind",
            )
        ],
    )
    def test_notebook_runner():
        print("*****HELLO FROM NOTEBOOK******")
        with open("/opt/airflow/notebooks/test_nb.ipynb", "r") as nb:
            print(nb.read())

    @task
    def check_notebook(nb_path, execution_date=None):
        import scrapbook as sb

        notebook = sb.read_notebook(nb_path)
        message = notebook.scraps["message"]
        if message.data != f"Ran from Airflow at {execution_date}":
            return False
        return True

    test_notebook_runner()
    notebook = run_notebook()
    check_notebook(notebook)


docker_taskflow()

if __name__ == "__main__":
    docker_taskflow()
