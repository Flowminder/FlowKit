from airflow.models import DagBag


def test_dags_present(airflow_dagbag):
    """
    Test that the correct dags are parsed
    """
    dags = airflow_dagbag.dags
    assert set(airflow_dagbag.dag_ids) == set(["etl", "etl_sensor"])
