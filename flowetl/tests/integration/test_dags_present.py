import pytest

from airflow.models import DagBag


def test_dags_present(airflow_local_setup_mdl_scope):
    """
    Test that the correct dags are parsed
    """
    assert set(DagBag(dag_folder="./dags", include_examples=False).dag_ids) == set(
        ["etl", "etl_sensor"]
    )


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
def test_correct_tasks(airflow_local_setup_mdl_scope, dag_name, expected_task_list):

    dag = DagBag(dag_folder="./dags", include_examples=False).dags[dag_name]
    assert set(dag.task_ids) == set(expected_task_list)
