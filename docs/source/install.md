Title: Installation

# Getting started with FlowKit

<a name="installationrequirements">

## Installation requirements

Most FlowKit components (FlowDB, FlowMachine, FlowAPI, FlowAuth, FlowETL, AutoFlow) are distributed as docker containers. To install these, you need:

- `docker >= 17.12.0`
- `docker-compose >= 1.21.0`

In addition, running FlowClient requires:

- `Python >= 3.6`

There are additional requirements for a [development setup](developer/dev_environment_setup.md), and we strongly recommend reviewing the [administrator section](administrator/index.md) in detail if you are planning to deploy FlowKit in production.

<a name="quickinstall">

## Quick install

This quick install guide will install the major components of FlowKit together with an initial setup and example analysis query.

The bulk of the installation process consists of using [Docker Compose](https://docs.docker.com/compose/) to download [Docker](https://docs.docker.com/install/) containers from [Docker Hub](https://hub.docker.com/u/flowminder), followed by a `pip install` of FlowClient.

These instructions assume use of [Pyenv](https://github.com/pyenv/pyenv) and [Pipenv](https://github.com/pypa/pipenv). If you are using [Anaconda](https://www.anaconda.com/what-is-anaconda/)-based installation commands may be different.

Docker containers for FlowAPI, FlowMachine, FlowDB, FlowAuth and the worked examples are provided in the Docker Hub repositories [flowminder/flowapi](https://hub.docker.com/r/flowminder/flowapi), [flowminder/flowmachine](https://hub.docker.com/r/flowminder/flowmachine), [flowminder/flowdb](https://hub.docker.com/r/flowminder/flowdb), [flowminder/flowauth](https://hub.docker.com/r/flowminder/flowauth), and [flowminder/flowkit-examples](https://hub.docker.com/r/flowminder/flowkit-examples) respectively. To install them, you will need [Docker](https://docs.docker.com/install/) and [Docker Compose](https://docs.docker.com/compose/install/).

Ensure that you have a Docker installed and running, then start the FlowKit test system by running

```bash
bash <(curl -s https://raw.githubusercontent.com/Flowminder/FlowKit/master/quick_start.sh)
```

This will pull any necessary docker containers, and start the system in the background with the API exposed on port `9090` by default, and the FlowAuth authentication system accessible by visiting <a href="http://localhost:9091/" target="_blank">http://localhost:9091</a> using your web browser.

The default system includes a small amount of test data. For a test system with considerably more data you can run

```bash
bash <(curl -s https://raw.githubusercontent.com/Flowminder/FlowKit/master/quick_start.sh) larger_data
```

!!! warning
    The larger data container will take considerably longer to start up, as it generates data when first run.

The [worked examples](worked_examples) are also available as part of the demo system. To install these run

```bash
bash <(curl -s https://raw.githubusercontent.com/Flowminder/FlowKit/master/quick_start.sh) examples smaller_data
```

for the examples with a small dataset, or

```bash
bash <(curl -s https://raw.githubusercontent.com/Flowminder/FlowKit/master/quick_start.sh) examples
```

to get the examples with the larger dataset (the one used when producing this documentation).

!!! info
    The small dataset is sufficient for most of the worked examples, but the larger dataset is required for the [Flows Above Normal](analyst/worked_examples/flows-above-normal.ipynb) example because this uses data for dates outside the range included in the small dataset.

!!! info
    The worked examples make use of [Mapbox GL](https://mapbox-mapboxgl-jupyter.readthedocs-hosted.com/en/latest/) for visualisation, which requires an API access token. If you would like to produce the maps in the worked examples notebooks, you will need to create a mapbox access token (following instructions [here](https://account.mapbox.com/)), and set this as the value of the `MAPBOX_ACCESS_TOKEN` environment variable before running the above commands.

To shut down the system, you can either stop all the docker containers directly, or run

```bash
bash <(curl -s https://raw.githubusercontent.com/Flowminder/FlowKit/master/quick_start.sh) stop
```

In order to use the test system, now [install FlowClient](#flowclient), and generate a token using FlowAuth.

### FlowAuth quickstart

Visit <a href="http://localhost:9091/" target="_blank">http://localhost:9091</a> and log in with either `TEST_ADMIN:DUMMY_PASSWORD` or `TEST_USER:DUMMY_PASSWORD`. `TEST_USER` is already set up to generate tokens for the FlowAPI instance started by the quick start script.

See the [administrator section](administrator/index.md#granting-user-permissions-in-flowauth) for details of how to add servers and users or modify user permissions, or the [analyst section](analyst/index.md#flowauth) for instructions to generate a token.

### FlowClient <a name="flowclient"> </a>

The FlowClient Python client is used to perform CDR analysis using the JupyterLab Python Data Science Stack. It may be installed using pip:

```bash
pip install flowclient
```

Quick install is continued with an example of FlowClient usage [here](analyst/index.md#flowclient).

If you are planning to hack on the codebase, you should also check out the [developer install](developer/dev_environment_setup.md) instructions; if you are ready to try FlowKit in production, visit the [production deployment](administrator/deployment.md) documentation to find out how to deploy FlowKit at your site.