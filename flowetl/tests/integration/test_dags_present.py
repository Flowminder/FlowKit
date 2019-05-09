import pytest


def test_dags_present(airflow_dagbag):
    """
    Test that the correct dags are parsed
    """
    dags = airflow_dagbag.dags
    assert set(airflow_dagbag.dag_ids) == set(["etl", "etl_sensor"])


@pytest.mark.parametrize(
    "dag_name,expected_task_list",
    [
        (
            "etl",
            [
                "init",
                "extract",
                "transform",
                "success_branch",
                "load",
                "archive",
                "quarantine",
                "clean",
                "fail",
            ],
        ),
        ("etl_sensor", ["sense"]),
    ],
)
def test_correct_tasks(airflow_dagbag, dag_name, expected_task_list):

    dag = airflow_dagbag.dags[dag_name]
    assert False
    assert set(dag.task_ids) == set(expected_task_list)
