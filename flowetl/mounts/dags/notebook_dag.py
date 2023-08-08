from datetime import datetime, timedelta, timezone
from airflow.decorators import task, dag
from airflow.providers.papermill.operators.papermill import PapermillOperator


@dag(
    dag_id="example_papermill_operator",
    default_args={"retries": 0},
    schedule="0 0 * * *",
    start_date=datetime(2023, 7, 25),
    template_searchpath="/usr/local/airflow/include",
    catchup=False,
)
def papermill_taskflow():
    notebook_task = PapermillOperator(
        task_id="example_notebook",
        input_nb="/opt/airflow/notebooks/test_nb.ipynb",
        output_nb="/opt/airflow/notebooks/out/out-{{ execution_date }}.ipynb",
        parameters={"execution_date": "{{ execution_date }}"},
    )

    @task
    def check_notebook(nb_path, execution_date=None):
        """
        verify that a notebook is there
        """
        import scrapbook as sb

        print("Reached check_notebook")
        print(nb_path)
        notebook = sb.read_notebook(nb_path[0].url)
        print(notebook.scraps)
        message = notebook.scraps["message"]
        print(f"Message in notebook {message} for {execution_date}")

        if message.data != f"Ran from Airflow at {execution_date}":
            return False
        return True

    check_notebook(notebook_task.output["pipeline_outlets"])


papermill_taskflow()
