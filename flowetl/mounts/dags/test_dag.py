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
    from airflow.providers.postgres.operators.postgres import PostgresOperator

    flowetl_test_op = BashOperator(
        task_id="flowetl_install_test_op", bash_command="date"
    )
    flowdb_test_op = PostgresOperator(
        task_id="flowdb_connect_test_op",
        sql="SELECT * FROM geography.geo_kinds",
        postgres_conn_id="flowdb",
    )
    flowdb_test_op >> flowetl_test_op
