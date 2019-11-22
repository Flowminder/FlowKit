# AutoFlow

AutoFlow is a tool that automates the event-driven execution of workflows consisting of Jupyter notebooks that interact with FlowKit via FlowAPI. Workflows can consist of multiple inter-dependent notebooks, and can run automatically for each new date of CDR data available in FlowDB). After execution, notebooks can optionally be converted to PDF reports.

AutoFlow uses:
- [Prefect](https://github.com/prefecthq/prefect) to define and run workflows,
- [Papermill](https://github.com/nteract/papermill) to parametrise and execute Jupyter notebooks,
- [Scrapbook](https://github.com/nteract/scrapbook) to enable data-sharing between notebooks,
- [nbconvert](https://github.com/jupyter/nbconvert) and [asciidoctor-pdf](https://github.com/asciidoctor/asciidoctor-pdf) to convert notebooks to PDF, via asciidoc.

## Docker container

### Build

To build the Docker image, run

```
docker build -t flowminder/autoflow .
```

### Run

When running the container, the following environment variables should be set:
- `AUTOFLOW_INPUTS_DIR`: Path to a directory containing Jupyter notebooks and a workflow definition file `workflows.yml`. This directory will be mounted as a read-only bind mount inside the container. See [Inputs](#Inputs) for details of what this directory should contain, and see `./examples/inputs/` for an example.
- `AUTOFLOW_OUTPUTS_DIR`: Path to a directory in which executed notebooks and PDF reports should be stored. This directory will be bind-mounted inside the container, and subdirectories `notebooks/` and `reports/` will be created if they do not already exist.
- `AUTOFLOW_LOG_LEVEL`: Logging level (e.g. 'DEBUG' or 'ERROR'). Must be upper-case.
- `AUTOFLOW_DB_URI`: URI of a SQL database in which AutoFlow will store metadata for workflow runs. The password can be replaced by `'{}'`, e.g. `postgresql://user:{}@localhost:5432/autoflow`. If database URI is not provided, a SQLite database will be used.
- `AUTOFLOW_DB_PASSWORD`: Password for the database.
- `FLOWAPI_URL`: FlowAPI URL.
- `FLOWAPI_TOKEN`: FlowAPI access token. This should allow access to `available_dates`, along with any query kinds used in the notebooks to be executed.

Alternatively, `AUTOFLOW_DB_PASSWORD` and `FLOWAPI_TOKEN` can be set as docker secrets instead of environment variables.

When the container runs, it will parse `workflows.yml` and construct all of the workflows defined, and run a sensor workflow that runs the workflows with the provided parameters each time new data is found in FlowDB. All executed notebooks will be available in `${AUTOFLOW_OUTPUTS_DIR}/notebooks/`, and PDF reports will be in `${AUTOFLOW_OUTPUTS_DIR}/reports/`.

## Inputs

### Defining workflows

Workflows should be defined in a yaml file `${AUTOFLOW_INPUTS_DIR}/workflows.yml`. This file should have thoe following structure:
- `workflows`: A sequence of workflow specifications. Each workflow specification has the following keys:
    - `name`: A unique name for this workflow.
    - `notebooks`: Specifications for one or more notebook execution tasks, defined as a mapping from labels to notebook task specifications. See the section on [defining notebook tasks](#defining-notebook-tasks) for more details.
- `available_dates_sensor`: Configuration parameters for the available dates sensor. This should have the following parameters:
    - `schedule`: A cron string describing the schedule on which the sensor will check for new available dates. Set `schedule: null` to run the sensor just once, without a schedule.
    - `cdr_types` (optional): A list of CDR types for which available dates should be checked (can be any subset of ["calls", "sms", "mds", "topups"]). Omit this parameter to check available dates for all available CDR types.
    - `workflows`: A list of sets of configuration parameters for workflows that the available dates sensor should trigger. Each element of this list should have the following parameters:
        - `workflow_name`: The name of a workflow defined in the `workflows` section at the top of `workflows.yml`.
        - `parameters`: Values of any parameters used by notebooks in the workflow (except `reference_date`, `date_ranges` and `flowapi_url`, which will be provided automatically).
        - `earliest_date` (optional): Optionally specify the earliest date of available data for which this workflow should run.
        - `date_stencil` (optional): Optionally provide a date stencil that defines a set of dates that must be available for this workflow to run. See the section on [date stencils](#date-stencils) for more details.

An example workflows definition file can be found in `./examples/inputs/workflows.yml`.

#### Defining notebook tasks

Jupyter notebooks in `$AUTOFLOW_INPUTS_DIR` can be executed as tasks within a workflow. The value of the `notebooks` parameter within a workflow specification in `workflows.yml` should be a mapping from notebook task keys (which notebook tasks can use to refer to each other) to notebook task specifications. Each notebook task specification has the following keys:
- `filename`: Filename of the jupyter notebook to be executed. This should refer to a file in `$AUTOFLOW_INPUTS_DIR`.
- `parameters`: A mapping that defines parameters for this notebook. Keys are the names of these parameters inside the notebook, and each value refers to one of:
    - The name of one of the parameters of this workflow.
    - The key of another notebook task in this workflow (i.e. the key corresponding to a notebook task specification). Dependencies between notebook tasks can be defined in this way, and these dependencies will be respected by the order in which notebooks are executed. The value of this parameter within the notebook will be the filename of the other notebook after execution.
    - The name of an 'automatic' workflow parameter (i.e. a parameter that will automatically be passed by the available dates sensor). There are two other 'automatic' parameters:
        - `reference_date`: The date of available CDR data for which the workflow is running (as an iso-format date string).
        - `date_ranges`: A list of pairs of iso-format date strings, describing the date ranges defined by the `date_stencil` (see the section on [date stencils](#date-stencils) below) for `reference_date`.
    - `flowapi_url`: The URL at which FlowAPI caln be accessed. The value of this parameter will be the URL set as the environment variable `FLOWAPI_URL` inside the container.
- `output` (optional): Set `output: {format: pdf}` to convert this notebook to PDF after execution. Omit the `output` key to skip converting this notebook to PDF. Optionally, a custom template can be used when converting the notebook to asciidoc by setting `output: {format: pdf, template: custom_asciidoc_template.tpl}` (where `custom_asciidoc_template.tpl` should be a file in `$AUTOFLOW_INPUTS_DIR`).

#### Date stencils

The default behaviour is to run a workflow for every date of available CDR data. If the notebooks refer to date periods other than the reference date for which the workflow is running, further filtering is required to ensure that all required dates are available. This can be done by providing a date stencil.

Each element of the list of workflows defined in the `available_dates_sensor` block in `workflows.yml` accepts a `date_stencil` parameter, which defines a pattern of dates relative to each available CDR date. The workflow will only run for a particular date if all dates contained in the date stencil for that date are available. The date stencil is a list of elements, each of which is one of:
- an absolute date (e.g.`2016-01-01`)
- an integer representing an offset (in days) from a reference date. For example, `-1` would refer to the day before a reference date, so a workflow containing `-1` would only run for date `2016-01-03` if date `2016-01-02` was available
- a pair of dates/offsets representing a date interval (inclusive of both limits) for which all dates must be available. Absolute dates and offsets can be mixed (e.g. `[2016-01-01, -3]`).

If the `date_stencil` parameter is not provided, the default value is `[0]` (i.e. only the reference date itself needs to be available).

For each date for which the workflow runs, the corresponding date ranges described by the date stencil are available to notebooks as the parameter `date_ranges`, which will contain a list of pairs of absolute dates. Note that `date_ranges` will always contain pairs of dates, even when the date stencil contains individual dates/offsets.

As an example, if the date stencil is
```yaml
- [2016-01-01, 2016-01-03]
- -3
- [-2, 0]
```
then the internal `date_ranges` parameter for reference date `2016-01-07` would have the value
```python
[
    ["2016-01-01", "2016-01-03"],
    ["2016-01-04", "2016-01-04"],
    ["2016-01-05", "2016-01-07"],
]
```

### Preparing notebooks for automated execution

#### Parameters

Notebooks are executed within workflows using [papermill](https://github.com/nteract/papermill), which enables passing parameters into a notebook. Within a notebook, indicate the cell containing parameters that should be replaced at execution time by adding the tag `parameters` to that cell (see [papermill's documentation](https://papermill.readthedocs.io/en/latest/usage-parameterize.html) for more details).

#### Passing data between notebooks

[Scrapbook](https://github.com/nteract/scrapbook) can be used to attach data to a notebook during execution, so that it can later be retrieved in another notebook.

To 'glue' the value of a variable `x` in a notebook:
```python
import scrapbook as sb
sb.glue("scrap_name", x)
```
This value can be retrieved in another notebook:
```python
import scrapbook as sb
nb = sb.read_notebook(path_to_first_notebook)
x = nb.scraps["scrap_name"].data
```

See [scrapbook's documentation](https://nteract-scrapbook.readthedocs.io/en/latest/index.html) for more details.

As an example, if the `notebooks` parameter of a workflow specification is
```yaml
notebooks:
  notebook1:
    filename: ...
    parameters: ...
  notebook2:
    filename: ...
    parameters:
      path_to_first_notebook: notebook1
      ...
```
then the `path_to_first_notebook` parameter in `notebook2` will be the path to the executed `notebook1`, so values 'glued' in `notebook1` can be accessed in `notebook2` using `sb.read_notebook(path_to_first_notebook)`.

#### Connecting to FlowAPI

Currently, the best way to create a FlowClient connection in a notebook is to pass the parameter `flowapi_url` to the notebook, and then run
```python
import flowclient
from get_secret_or_env_var import environ
conn = flowclient.connect(url=flowapi_url, token=environ["FLOWAPI_TOKEN"])
```

**Note:** This method for accessing the FlowAPI access token is likely to change in the future.

The example notebooks in `./examples/inputs` contain examples of parameterisation with papermill, data-sharing with scrapbook, and creating a FlowClient connection.

## Running outside a container

To run AutoFlow outside of a Docker container, run

```bash
pipenv install
pipenv run python -m autoflow
```

In addition to the environment variables above, `PREFECT__USER_CONFIG_PATH=./config/config.toml` must be set. To use the extended asciidoc template (as used when running in the Docker container), `PREFECT__ASCIIDOC_TEMPLATE_PATH=./config/asciidoc_extended.tpl` should also be set.


## Contents

- `autoflow/` - Python package for running notebooks workflows. Contains the following modules:
    - `__main__.py` - Main script that runs `app.main()`. Run using `python -m autoflow`.
    - `app.py` - Defines the `main` function, that initialises the database, parses the input file and runs workflows.
    - `date_stencil.py` - Defines a `DateStencil` class to represent date stencils.
    - `model.py` - Defines a database model for storing workflow run metadata.
    - `parser.py` - Functions for parsing workflow definition files.
    - `sensor.py` - Defines the `available_dates_sensor` prefect flow, which can be configured to run other workflows whenever new days of data become available.
    - `utils.py` - Various utility functions.
    - `workflows.py` - Prefect tasks used in workflows, and a `make_notebooks_workflow` function to create a prefect flow that parametrises and executes notebooks.
- `config/` - Directory containing the following configuration files:
    - `config.toml` - Prefect user configuration file. Defines config values available to prefect tasks during execution.
    - `asciidoc_extended.tpl` - Extends the default `nbconvert` asciidoc template. This template will be used when converting notebooks to PDF, unless the user provides a different template.
- `examples/` - Contains inputs for an example date-triggered workflow that produces a PDF report of flows above normal for each day of CDR data. Running `make automation-up` in the FlowKit root directory will start a AutoFlow container that runs this example workflow.
    - `inputs/` - The `AUTOFLOW_INPUTS_DIR` should point to this directory when running the example. It contains:
        - `run_flows.ipynb` - Notebook that runs two `flows` queries, so that they are stored in cache. The query IDs for the `flows` queries are glued in this notebook.
        - `flows_Report.ipynb` - Notebook that reads the query IDs from `run_flows.ipynb`, gets the query results using FlowClient and combines them with geography data, and displays the flows above normal in plots and tables. This notebook is designed to be converted to a PDF report after execution.
        - `workflows.yml` -  Yaml file that defines a workflow that runs `run_flows.ipynb` followed by `flows_report.ipynb` and creates a PDF report from `flows_report.ipynb`, and configures the available dates sensor to run this workflow.
    - `outputs/` - The `AUTOFLOW_OUTPUTS_DIR` should point to this directory when running the example. Executed notebooks and PDF reports will be saved to subdirectories `notebooks/` and `reports/`, respectively.
- `tests/` - Tests
- `Dockerfile` - Dockerfile for building the `flowminder/autoflow` image.
- `docker-stack.yml` - Example docker stack file that could be used to start AutoFlow using docker secrets. Note that for real usage this docker stack would also need to be connected to a network on which a FlowAPI is accessible.
- `Pipfile` - Pipfile that can be used to create a pipenv environment for running AutoFlow locally (i.e. outside a container). The Pipfile is not used when building the docker image.
