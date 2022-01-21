from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator

# TODO Tomorrow: Design the ingestion DAG
with DAG() as dag:

    sighting_task = PostgresOperator(sql=Path("../sql/create_sightings_table.sql"))
    other_task = PythonOperator(SightingTask)
    sighting_task >> other_task  # Hey, you can define DAGs using operators!
