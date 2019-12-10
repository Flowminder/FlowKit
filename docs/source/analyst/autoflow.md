# AutoFlow

AutoFlow is a tool that automates the event-driven execution of workflows consisting of Jupyter notebooks that interact with FlowKit via FlowAPI. Workflows can consist of multiple inter-dependent notebooks, and can run automatically for each new date of CDR data available in FlowDB. After execution, notebooks can optionally be converted to PDF reports.

AutoFlow consists of a sensor that, on a user-defined schedule, checks for new available dates of CDR data in a FlowKit instance, and runs a set of notebook-based workflows for each date for which they haven't previously run successfully. Each notebook-based workflow is a user-defined workflow that executes one or more Jupyter notebooks, and optionally converts the executed notebooks to PDF.

## Running AutoFlow

AutoFlow is designed to run in a Docker container (defined by the `flowminder/autoflow` image) with two bind-mounted directories:

- `/mounts/inputs`: Inputs directory (read-only), which contains the notebooks to be executed along with a YAML file `workflows.yml` that defines the workflows to execute the notebooks and configures the available dates sensor to run these workflows. See the [inputs section](#inputs) for details of what this directory should contain, and see the [example section](#example) for an example.  
- `/mounts/outputs`: Outputs directory, which will contain two sub-directories `notebooks/` and `reports/` containing the executed notebooks and PDF reports, respectively. The sub-directories will be created if they do not already exist.  

When running the container, the following environment variables should be set:

- `AUTOFLOW_LOG_LEVEL`: Logging level (e.g. 'DEBUG' or 'ERROR'). Must be upper-case.  
- `AUTOFLOW_DB_URI`: URI of a SQL database in which AutoFlow will store metadata for workflow runs. The password can be replaced by `'{}'`, e.g. `postgresql://user:{}@localhost:5432/autoflow`. If database URI is not provided, a temporary SQLite database will be created inside the container.  
- `AUTOFLOW_DB_PASSWORD`: Password for the database.  
- `FLOWAPI_URL`: FlowAPI URL.  
- `FLOWAPI_TOKEN`: FlowAPI access token. This should allow access to `available_dates`, along with any query kinds used in the notebooks to be executed.  

Alternatively, `AUTOFLOW_DB_PASSWORD` and `FLOWAPI_TOKEN` can be set as docker secrets instead of environment variables.

When the container runs, it will parse `workflows.yml` and construct all of the workflows defined, and run a sensor that runs the workflows with the provided parameters each time new data is found in FlowDB. All executed notebooks will be available in `<bind-mounted outputs directory>/notebooks/`, and PDF reports will be in `<bind-mounted outputs directory>/reports/`.

## Inputs

### Defining workflows

Workflows should be defined in a yaml file `workflows.yml`, in the inputs directory that will be bind-mounted into the AutoFlow container. This file should have the following structure:

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

See the [example section](#example) for an example.

#### Defining notebook tasks

Jupyter notebooks in the inputs directory can be executed as tasks within a workflow. The value of the `notebooks` parameter within a workflow specification in `workflows.yml` should be a mapping from notebook task keys (which notebook tasks can use to refer to each other) to notebook task specifications. Each notebook task specification has the following keys:

- `filename`: Filename of the Jupyter notebook to be executed. This should refer to a file in the same directory as `workflows.yml`.  
- `parameters`: A mapping that defines parameters for this notebook. Keys are the names of these parameters inside the notebook, and each value refers to one of:  
    - The name of one of the parameters of this workflow.  
    - The key of another notebook task in this workflow (i.e. the key corresponding to a notebook task specification). Dependencies between notebook tasks can be defined in this way, and these dependencies will be respected by the order in which notebooks are executed. The value of this parameter within the notebook will be the filename of the other notebook after execution.  
    - The name of an 'automatic' workflow parameter (i.e. a parameter that will automatically be passed by the available dates sensor). There are two 'automatic' parameters:  
        - `reference_date`: The date of available CDR data for which the workflow is running (as an iso-format date string).  
        - `date_ranges`: A list of pairs of iso-format date strings, describing the date ranges defined by the `date_stencil` (see the section on [date stencils](#date-stencils) below) for `reference_date`.  
    - `flowapi_url`: The URL at which FlowAPI can be accessed. The value of this parameter will be the URL set as the environment variable `FLOWAPI_URL` inside the container.  
- `output` (optional): Set `output: {format: pdf}` to convert this notebook to PDF after execution. Omit the `output` key to skip converting this notebook to PDF. Optionally, a custom template can be used when converting the notebook to asciidoc by setting `output: {format: pdf, template: custom_asciidoc_template.tpl}` (where `custom_asciidoc_template.tpl` should be a file in the same directory as `workflows.yml`).  

#### Date stencils

The default behaviour is to run a workflow for every date of available CDR data. If the notebooks refer to date periods other than the reference date for which the workflow is running, further filtering is required to ensure that all required dates are available. This can be done by providing a date stencil.

Each element of the list of workflows defined in the `available_dates_sensor` block in `workflows.yml` accepts a `date_stencil` parameter, which defines a pattern of dates relative to each available CDR date. The workflow will only run for a particular date if all dates contained in the date stencil for that date are available. The date stencil is a list of elements, each of which is one of:

- an absolute date (e.g.`2016-01-01`);  
- an integer representing an offset (in days) from a reference date. For example, `-1` would refer to the day before a reference date, so a workflow containing `-1` would only run for date `2016-01-03` if date `2016-01-02` was available;  
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

!!! note
    This method for accessing the FlowAPI access token is likely to change in the future.

The example notebooks in the [example section](#example) contain examples of parameterisation with papermill, data-sharing with scrapbook, and creating a FlowClient connection.

## Example

[This directory](https://github.com/Flowminder/FlowKit/tree/master/autoflow/examples/inputs) contains example inputs for an AutoFlow workflow that runs two Jupyter notebooks and produces a PDF report showing in/outflows above normal for each day of CDR data. It contains two Jupyter notebooks:

- `run_flows.ipynb`: Notebook that runs two `flows` queries, so that they are stored in cache. The query IDs for the `flows` queries are glued in this notebook.  
- `flows_report.ipynb`: Notebook that reads the query IDs from `run_flows.ipynb`, gets the query results using FlowClient and combines them with geography data, and displays the flows above normal in plots and tables. This notebook is designed to be converted to a PDF report after execution.  

and an input file `workflows.yml` that defines a workflow that runs `run_flows.ipynb` followed by `flows_report.ipynb` and creates a PDF report from `flows_report.ipynb`, and configures the available dates sensor to check for new dates every day at midnight, and run the workflow with two different sets of parameters.

To run the AutoFlow example:

1. Start a FlowKit instance, by following the [quick-start instructions](../install.md#quickinstall).  
2. Go to <a href="http://localhost:9091/" target="_blank">FlowAuth</a>, log in with username `TEST_USER` and password `DUMMY_PASSWORD`, and create an API token following instructions [here](index.md#flowauth).  
3. Create two empty directories, one for inputs and the other for outputs.  
4. Copy the example input files into your inputs directory:  
```bash
cd <your_inputs_directory>
wget https://raw.githubusercontent.com/Flowminder/FlowKit/master/autoflow/examples/inputs/run_flows.ipynb
wget https://raw.githubusercontent.com/Flowminder/FlowKit/master/autoflow/examples/inputs/flows_report.ipynb
wget https://raw.githubusercontent.com/Flowminder/FlowKit/master/autoflow/examples/inputs/workflows.yml
```
5. Start AutoFlow:  
```bash
docker run -d \
    -v <your_inputs_directory>:/mounts/inputs:ro \
    -v <your_outputs_directory>:/mounts/outputs:rw \
    -e "AUTOFLOW_LOG_LEVEL=ERROR" \
    -e "FLOWAPI_URL=http://localhost:9090" \
    -e "FLOWAPI_TOKEN=<api_token>" \
    flowminder/autoflow:latest
```
(where `<your_inputs_directory>` and `<your_outputs_directory>` are the paths to the input and output directories you created, and `<api_token>` is the token you created in FlowAuth).

To run the sensor immediately without a schedule, rather than waiting until midnight, change the line `schedule: "0 0 * * *"` to `schedule: null` in `workflows.yml`.

Outputs will appear in the outputs directory you created.
