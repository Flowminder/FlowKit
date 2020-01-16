from pathlib import Path
from shutil import copytree, rmtree

import click


@click.command()
@click.argument("dag_folder", type=click.Path(exists=True))
def main(dag_folder):
    """
    Copy the module into the dag folder, allowing it to be picked up by airflow.

    Parameters
    ----------
    dag_folder : str
        Path to the dag folder to copy the module to.

    """
    dag_folder = Path(dag_folder)
    try:
        rmtree(dag_folder / "flowetl")
    except FileNotFoundError:
        pass  # No need to remove before copying
    copytree(Path(__file__).parent, dag_folder / "flowetl")
