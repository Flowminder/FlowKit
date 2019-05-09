from airflow.models import DagBag


def test_dags_present(airflow_dagbag):
    dags = airflow_dagbag.dags
    assert set(airflow_dagbag.dag_ids) == set(["etl", "etl_sensor"])
