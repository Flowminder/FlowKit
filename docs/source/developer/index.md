# Information for Developers

Because FlowKit deployment is primarily done using Docker, the installation for developers is slightly different, see the instructions [here](../install.md#developers).

An outline roadmap is provided below together with details about [contributing to the project](#contrib).

Followed by a guide to the [FlowAPI](#flowapi).

FlowMachine specifications are found [here](#flowmachine).

FlowDB details are found [here](#flowdb).

FlowETL details are found [here](#flowetl).

<a href="roadmap"></a>

## Roadmap

### Now

-   Additional FlowMachine aggregates exposed via API
-   Non-spatial aggregations
-   Documentation enhancements

### Next

-   New metrics and insights
-   Custom geography support

### Later


-   Additional language targets for FlowClient
-   Alternative login provider support for FlowAuth
-   Plugin support
-   Enhanced temporal aggregations
-   Individual level API

<a name="contrib"></a>
<p></p>
<p></p>

## Contributing

We are creating FlowKit at [Flowminder](http://flowminder.org).

### Get involved
You are welcome to contribute to the FlowKit library. To get started, please check out our [contributor guidelines](https://github.com/Flowminder/FlowKit/blob/master/.github/CONTRIBUTING.md), and when you're ready to get started, follow the [developer install guide](dev_environment_setup.md).


<a name="flowapi"></a>

## FlowAPI Guide

FlowAPI is an HTTP API which provides access to the functionality of [FlowMachine](#flowmachine). A full overview of the available api routes is available as an [interactive api specification](api-spec.html).

FlowAPI uses [ZeroMQ](http://zeromq.org/) for asynchronous communication with the FlowMachine server.

### FlowAPI Access tokens

As explained in the [quick install guide](../install.md#quickinstall), user authentication and access control are handled through the use of [JSON Web Tokens (JWT)](http://jwt.io). There are two categories of permissions (`run` and `get_result`) which can be granted to a user.
JWTs allow these access permissions to be granted independently for each query kind (e.g. get results for daily location only at admin 3 resolution). The FlowAuth authentication management system is designed to generate JWTs for accessing FlowAPI.

#### Test Tokens

FlowKit includes the `flowkit-jwt-generator` package, which can be used to generate tokens for testing purposes. This package supplies:

- Two commandline tools
    - `generate-jwt`, which allows you to generate tokens which grant specific kinds of access to a subset of queries, or to generate an all access token for a specific instance of FlowAPI
- Two pytest plugins
    - `access_token_builder`
    - `universal_access_token`

Alternatively, the `generate_token` function can be used directly.


<a name="flowmachine">

## FlowMachine

FlowMachine is a Python toolkit for the analysis of CDR data. It is essentially a python wrapper for postgres SQL queries, including geographical queries with postgis. All of the classes in flowmachine define an SQL query under the hood.

### Documentation

Documentation for FlowMachine can be found [here](../flowmachine/flowmachine/). A worked example of using FlowMachine for analysis is provided [here](../analyst/advanced_usage/worked_examples/mobile-data-usage/).


<a name="flowdb">

## FlowDB

FlowDB is database designed for storing, serving, and analysing mobile operator data. The project's main features are:

-   Uses standard schema for most common mobile operator data
-   Is built as a [Docker](http://docker.com/) container
-   Uses [PostgreSQL 12](https://www.postgresql.org/docs/11/static/release-12.html)
-   Grants different permissions for users depending on need
-   Is configured for high-performance operations
-   Comes with
    -   [PostGIS](https://postgis.net) for geospatial data handling and analysis
    -   [pgRouting](http://pgrouting.org) for advanced spatial analysis
    -   Utilities for connecting with Oracle databases ([oracle_fdw](https://github.com/laurenz/oracle_fdw))

### Synthetic Data

In addition to the bare FlowDB container and the test data container used for tests a 'synthetic' data container is available. This container generates arbitrary quantities of data at runtime.

Two data generators are available - a Python based generator, which supports reproducible random data, and an SQL based generator which supports greater data volumes, more data types, plausible subscriber behaviour, and simple disaster scenarios.

Both are packaged in the `flowminder/flowdb-synthetic-data` docker image, and configured via environment variables.

For example, to generate a repeatable random data set with seven days of data, where 20,000 subscribers make 10,000 calls each day and use 5,000 cells:

```bash
docker run --name flowdb_synth_data -e FLOWMACHINE_FLOWDB_PASSWORD=foo -e FLOWAPI_FLOWDB_PASSWORD=foo \
 --publish 9000:5432 \
 -e N_CALLS=10000 -e N_SUBSCRIBERS=20000 -e N_CELLS=5000 -e N_DAYS=7 -e SYNTHETIC_DATA_GENERATOR=python \
 -e SUBSCRIBERS_SEED=11111 -e CALLS_SEED=22222 -e CELLS_SEED=33333 \
 --detach flowminder/flowdb-synthetic-data:latest
```

Or to generate an equivalent data set which includes TACs, mobile data sessions and sms:

```bash
docker run --name flowdb_synth_data -e FLOWMACHINE_FLOWDB_PASSWORD=foo -e FLOWAPI_FLOWDB_PASSWORD=foo \
 --publish 9000:5432 \
 -e N_CALLS=10000 -e N_SUBSCRIBERS=20000 -e N_CELLS=5000 -e N_SITES=5000 -e N_DAYS=7 -e SYNTHETIC_DATA_GENERATOR=sql \
 -e N_SMS=10000 -e N_MDS=10000 \
 --detach flowminder/flowdb-synthetic-data:latest
```

!!! warning
    For generating large datasets, it is recommended that you use the SQL based generator.

#### SQL Generator features

The SQL generator supports semi-plausible behaviour - each subscriber has a 'home' region, and will typically (by default, 95% of the time) call/sms/use data from cells in that region. Subscribers will occasionally (by default, 1% chance per day) relocate to a new home region.
Subscribers also have a consistent phone model across time, and a consistent set of other subscribers who they interact with (by default, `5*N_SUBSCRIBERS` calling pairs are used).

Mass relocation scenarios are also supported - a designated admin 2 region can be chosen to be 'off limits' to all subscribers for a period. Any subscribers ordinarily resident will relocate to another randomly chosen region, and no subscriber will call from a cell within the region or relocate there while the region is off limits.

#### Parameters

- `N_DAYS`: number of days of data to generate, defaults to 7
- `N_SUBSCRIBERS`: number of simulated subscribers, defaults to 4,000
- `N_TACS`: number of mobile phone models, defaults to 1,000 (SQL generator only)
- `N_SITES`: number of mobile sites, defaults to 1,000 (SQL generator only)
- `N_CELLS`: number of cells, defaults to 1,000
- `N_CALLS`: number of calls to generate per day, defaults to 200,000
- `N_SMS`: number of sms to generate per day, defaults to 200,000 (SQL generator only)
- `N_MDS`: number of mobile data sessions to generate per day, defaults to 200,000 (SQL generator only)
- `SUBSCRIBERS_SEED`: random seed used when generating subscribers, defaults to 11111 (Python generator only)
- `CALLS_SEED`: random seed used when generating calls, defaults to 22222 (Python generator only)
- `CELLS_SEED`: random seed used when generating cells, defaults to 33333 (Python generator only)
- `SYNTHETIC_DATA_GENERATOR`: which data generator to use, may be`'sql'` or `'python'`. Defaults to `'sql'`
- `P_OUT_OF_AREA`: probability that an event is taking place out of a subscriber's home region. Defaults to 0.05
- `P_RELOCATE`: probability that each subscriber relocates each day, defaults to 0.01
- `INTERACTIONS_MULTIPLIER`: multiplier for interaction pairs, defaults to 5.


## FlowAuth

### Quick setup to run the Frontend tests interactively

For development purposes, it is useful to be able to run the Flowauth frontend tests interactively.

- As an initial step, ensure that all the relevant Python and Javascript dependencies are installed.
```
cd /path/to/flowkit/flowauth/
pipenv install

cd /path/to/flowkit/flowauth/frontend
npm install
```

- The following command sets both the flowauth backend and frontend running (and also opens the flowauth web interface at `http://localhost:3000/` in the browser).
```
cd /path/to/flowkit/flowauth/
pipenv run start-all
```

- To open the Cypress UI, run the following in a separate terminal session:
```
cd /path/to/flowkit/flowauth/frontend/
npm run cy:open
```

- You can then click the button "Run all specs", or select an individual spec to run only a subset of the tests.

## AutoFlow

AutoFlow is a tool that automates the event-driven execution of workflows consisting of Jupyter notebooks that interact with FlowKit via FlowAPI. Workflows can consist of multiple inter-dependent notebooks, and can run automatically for each new date of CDR data available in FlowDB. After execution, notebooks can optionally be converted to PDF reports.

AutoFlow uses:

- [Prefect](https://github.com/prefecthq/prefect) to define and run workflows,  
- [Papermill](https://github.com/nteract/papermill) to parametrise and execute Jupyter notebooks,  
- [Scrapbook](https://github.com/nteract/scrapbook) to enable data-sharing between notebooks,  
- [nbconvert](https://github.com/jupyter/nbconvert) and [asciidoctor-pdf](https://github.com/asciidoctor/asciidoctor-pdf) to convert notebooks to PDF, via asciidoc.  

There are two categories of workflow used within AutoFlow:

- Notebook-based workflow: A Prefect flow that executes one or more Jupyter notebooks using Papermill, and optionally converts the executed notebooks to PDF. These are created using the function `autoflow.workflows.make_notebooks_workflow`.  
- Sensor: A Prefect flow that runs on a schedule, checks for new events, and runs a set of notebook-based workflows for each event for which they haven't previously run successfully. One sensor is currently implemented - `autoflow.sensor.available_dates_sensor`, which uses the FlowAPI get_available_dates route to get a list of available dates of CDR data, and runs notebook-based workflows for each new date found.  

To run AutoFlow outside a container, change into the `autoflow/` directory and run
```bash
pipenv install
pipenv run python -m autoflow
```

In addition to the environment variables described in the [AutoFlow documentation](../analyst/autoflow.md#running-autoflow), the following environment variables should be set to run AutoFlow outside a container:
```
AUTOFLOW_INPUTS_DIR=./examples/inputs
AUTOFLOW_OUTPUTS_DIR=./examples/outputs
PREFECT__USER_CONFIG_PATH=./config/config.toml
PREFECT__ASCIIDOC_TEMPLATE_PATH=./config/asciidoc_extended.tpl
```
