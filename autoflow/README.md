# AutoFlow

AutoFlow is a tool that automates the event-driven execution of workflows consisting of Jupyter notebooks that interact with FlowKit via FlowAPI. Workflows can consist of multiple inter-dependent notebooks, and can run automatically for each new date of CDR data available in FlowDB. After execution, notebooks can optionally be converted to PDF reports.

AutoFlow uses:
- [Prefect](https://github.com/prefecthq/prefect) to define and run workflows,
- [Papermill](https://github.com/nteract/papermill) to parametrise and execute Jupyter notebooks,
- [Scrapbook](https://github.com/nteract/scrapbook) to enable data-sharing between notebooks,
- [nbconvert](https://github.com/jupyter/nbconvert) and [asciidoctor-pdf](https://github.com/asciidoctor/asciidoctor-pdf) to convert notebooks to PDF, via asciidoc.

Documentation for AutoFlow can be found [here](https://flowkit.xyz/analyst/autoflow/).

## Example

`autoflow/examples/` contains an example AutoFlow workflow, which runs two Jupyter notebooks and produces a PDF report showing in/outflows above normal for each day of CDR data. `autoflow/examples/inputs/workflows.yml` configures the available dates sensor to check for new dates every day at midnight, and run the flows-above-normal workflow with two different sets of parameters.

The AutoFlow example can be run from the FlowKit root by first creating a FlowAPI token, and then running

```bash
set -a && . development_environment && set +a
export FLOWAPI_TOKEN=<token>
make up DOCKER_SERVICES="flowdb_testdata flowmachine flowmachine_query_locker flowapi autoflow"
```

Outputs will be in `autoflow/examples/outputs/`.

## Contents

- `autoflow/` - Python package for running notebooks workflows. Contains the following modules:
    - `__main__.py` - Main script that runs `app.main()`. Run using `python -m autoflow`.
    - `app.py` - Defines the `main` function, that initialises the database, parses the input file and runs workflows.
    - `date_stencil.py` - Defines a `DateStencil` class to represent date stencils.
    - `model.py` - Defines a database model for storing workflow run metadata.
    - `parser.py` - Functions for parsing workflow definition files.
    - `sensor.py` - Defines the `available_dates_sensor` prefect flow, which can be configured to run other workflows when new days of data become available.
    - `utils.py` - Various utility functions.
    - `workflows.py` - Prefect tasks used in workflows, and a `make_notebooks_workflow` function to create a prefect flow that parametrises and executes notebooks.
- `config/` - Directory containing the following configuration files:
    - `config.toml` - Prefect user configuration file. Defines config values available to prefect tasks during execution.
    - `asciidoc_extended.tpl` - Extends the default `nbconvert` asciidoc template. This template will be used when converting notebooks to PDF, unless the user provides a different template.
- `examples/` - Contains inputs for an example date-triggered workflow that produces a PDF report of flows above normal for each day of CDR data. Running `make autoflow-up` in the FlowKit root directory will start a AutoFlow container that runs this example workflow.
    - `inputs/` - The `AUTOFLOW_INPUTS_DIR` environment variable should point to this directory when running the example. It contains:
        - `run_flows.ipynb` - Notebook that runs two `flows` queries, so that they are stored in cache. The query IDs for the `flows` queries are glued in this notebook.
        - `flows_report.ipynb` - Notebook that reads the query IDs from `run_flows.ipynb`, gets the query results using FlowClient and combines them with geography data, and displays the flows above normal in plots and tables. This notebook is designed to be converted to a PDF report after execution.
        - `workflows.yml` -  Yaml file that defines a workflow that runs `run_flows.ipynb` followed by `flows_report.ipynb` and creates a PDF report from `flows_report.ipynb`, and configures the available dates sensor to run this workflow.
    - `outputs/` - The `AUTOFLOW_OUTPUTS_DIR` environment variable should point to this directory when running the example. Executed notebooks and PDF reports will be saved to subdirectories `notebooks/` and `reports/`, respectively.
- `tests/` - Unit tests.
- `Dockerfile` - Dockerfile for building the `flowminder/autoflow` image.
- `docker-stack.yml` - Example docker stack file that could be used to start AutoFlow using docker secrets. Note that for real usage this docker stack would also need to be connected to a network on which a FlowAPI is accessible.
- `Pipfile` - Pipfile that can be used to create a pipenv environment for running AutoFlow locally (i.e. outside a container). The Pipfile is not used when building the docker image.
