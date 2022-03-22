# A DAG to check that deployment of flowETL

from airflow import DAG
from pendulum import now

with DAG(
    "test_dag",
    start_date=now().subtract(days=3),
    schedule_interval="@daily",
    catchup=True,
) as dag:
    from airflow.operators.bash import BashOperator

    op = BashOperator(task_id="dummy", bash_command="date")
