Title: Developer install

# Developer install

## Installation requirements

Just as for a regular installation, you will need `docker` and `docker-compose` (see [Installation requirements](#installationrequirements) above).

During development, you will typically also want to run FlowMachine, FlowAPI, FlowAuth and/or AutoFlow outside docker containers. This requires additional prerequisites to be available.

- [Pipenv](https://pipenv.readthedocs.io/en/latest/) (to manage separate pipenv environment for each FlowKit component)
- FlowMachine server: `Python >= 3.7`
- FlowAuth: `npm` (we recommend installing it via [nvm](https://github.com/nvm-sh/nvm)); [Cypress](https://www.cypress.io/) for testing
- AutoFlow: [pandoc](https://pandoc.org/installing.html), `Ruby` (we recommend installing Ruby via [RVM](https://rvm.io/)), and [Bundler](https://bundler.io/) (to manage Ruby package dependencies).

## Setting up FlowKit for development

After cloning the [GitHub repository](https://github.com/Flowminder/FlowKit), the FlowKit system can be started by running `set -a && . development_environment && set +a` (this will set the required environment variables) followed by `make up` in the root directory. This requires [Docker](https://docs.docker.com/install/) and [Docker Compose](https://docs.docker.com/compose/install/) to be installed, and starts the FlowKit docker containers using the `docker-compose.yml` file. The docker containers to start can be selected by running `make up DOCKER_SERVICES="<service 1> <service 2> ..."`, where the services can be any of `flowmachine`, `flowmachine_query_locker`, `flowapi`, `flowauth`, `flowdb`, `worked_examples`, `flowdb_testdata`, `flowdb_synthetic_data`, `flowetl`, `flowetl_db` or `autoflow` (at most one of the `flowdb` containers). The default is to start `flowdb`, `flowapi`, `flowmachine`, `flowauth`, `flowmachine_query_locker`, `flowetl`, `flowetl_db` and `worked_examples`. Alternatively, containers can be built, started or stopped individually by running `make <service>-build`, `make <service>-up` or `make <service>-down`, respectively.

To run the `autoflow` service, a valid FlowAPI token must be set as the environment variable `FLOWAPI_TOKEN` (in addition to the environment variables set by running `set -a && . development_environment && set +a`).

FlowKit uses [pipenv](https://pipenv.readthedocs.io/) to manage Python environments. To start a Python session in which you can use FlowClient:

```bash
cd flowclient
pipenv install
pipenv run python
>>> import flowclient
```

To run the tests in the `flowapi`, `flowclient`, `flowdb`, `flowmachine`, `autoflow`, `flowetl` or `integration_tests` directory:

```bash
cd <directory>
pipenv install --dev
pipenv run pytest
```

AutoFlow additionally has dependencies on Ruby packages, which we manage using [Bundler](https://bundler.io/) which works similarly to pipenv. To run the tests in the `autoflow` directory:

```bash
cd autoflow
bundle install
pipenv install --dev
pipenv run pytest
```


## Getting set up to contribute

### Prerequisites

#### Pre-commit hook (for Python code formatting with black)

FlowKit's Python code is formatted with [black](https://black.readthedocs.io/en/stable/) (which is included in the
FlowKit pipenv environments). There is also a [pre-commit](https://pre-commit.com/) hook which runs black on all Python
files before each commit.

To install the pre-commit hook run:
```bash
pre-commit install
```
If you ever want to uninstall the hook you can do this by running `pipenv run pre-commit uninstall`.

The above command requires `pre-commit` to be installed on your system. For convenience, it is also
included in the `flowmachine` pipenv environment, so if you don't want to install it system-wide you
can run it as follows (it will still be installed and available for all FlowKit components, not just
flowmachine).
```bash
cd flowmachine/
pipenv run pre-commit install
```

Note that if you run `git commit` and any files are modified by the re-formatting, the pre-commit hook will abort
the commit (but leave the files re-formatted). Simply repeat the `git commit` command in order to complete the commit.

Example:

```bash
# 1) First attempt to commit; this is aborted because one of the files
#    is reformatted by the pre-commit hook.

$ git commit -a -m "Some changes"
black...................................................Failed
hookid: black

Files were modified by this hook. Additional output:

reformatted flowmachine/__init__.py
All done! ✨ 🍰 ✨
1 file reformatted.


# 2) Complete the commit by running the same 'git commit' command again.

$ git commit -a -m "Some changes"
black...................................................Passed
[black-pre-commit-hook 8ebaace] Some changes
 1 file changed, 2 insertions(+), 1 deletion(-)
```

#### Diff tool (for verifying changes in `ApprovalTests`-based tests)

Some of the tests use [ApprovalTests](https://github.com/approvals/ApprovalTests.Python) to verify large output against
a known "approved" version (stored in files called `*approved.txt`). For example, the API specification is
verified in this way. If you make code changes that alter the results of these tests, the content of the
relevant `*.approved.txt` file needs to be updated. ApprovalTests will do this automatically for you, but it may be useful to have a
diff tool installed. (See here for some recommendations for diff tools on [Mac](`https://www.git-tower.com/blog/diff-tools-mac`)
and [Windows](https://www.git-tower.com/blog/diff-tools-windows).) The reporters supported out of the box by ApprovalTests can be found [here](https://github.com/approvals/ApprovalTests.Python/blob/master/approvaltests/reporters/reporters.json).

To use a specific diff reporter, you should create files named `reporters.json` in `flowmachine/tests` and `integration_tests` following the format shown in the ApprovalTests [README](https://github.com/approvals/ApprovalTests.Python).

!!! warning
    The project root contains an env file (`development_environment`), in which are default values for _all_ of the environment variables used to control and configure the FlowDB, FlowAPI, FlowMachine and FlowAuth. You will need to source this file (`set -a && . ./development_environment && set +a`), or otherwise set the environment variables before running _any other command_.

### Option 1: Starting up all FlowKit components inside a dockerised development environment

For convenience, FlowKit comes with a dockerised development environment. You can start up a development version of all
components by running:
```bash
make up
```

This will use the specifications in `docker-compose.yml` to spin up development versions of `flowdb_testdata`,
`flowmachine`, `flowapi` and `flowauth` (as well as a `redis` container, which is used internally by `flowmachine`).

The first time you run this command it will build the docker images locally, which will take ~15 minutes. Subsequent
runs will be much faster because it will typically only need to (re-)start the containers, or re-build small parts
of the images.

Here is an example of the running docker services after a successful `make up`:
```
$ docker ps

CONTAINER ID        IMAGE                               COMMAND                  CREATED             STATUS              PORTS                              NAMES
54857c5f5da5        flowminder/flowdb-testdata:latest   "docker-entrypoint.s…"   5 minutes ago       Up 5 minutes        8000/tcp, 0.0.0.0:9000->5432/tcp   flowdb_testdata
e57c8eeb7d38        flowminder/flowmachine:latest       "/bin/sh -c 'pipenv …"   5 minutes ago       Up 5 minutes        0.0.0.0:5555->5555/tcp             flowmachine
3d54ce0c4484        flowminder/flowapi:latest           "/bin/sh -c 'pipenv …"   5 minutes ago       Up 5 minutes        0.0.0.0:9090->9090/tcp             flowapi
89e9575c8d42        flowminder/flowauth:latest          "/entrypoint.sh /sta…"   5 minutes ago       Up 5 minutes        443/tcp, 0.0.0.0:8080->80/tcp      flowauth
384355e94ae6        bitnami/redis                       "/entrypoint.sh /run…"   5 minutes ago       Up 5 minutes        0.0.0.0:6379->6379/tcp             flowmachine_query_locker
```

**Note:** _Currently there is a glitch which means that `flowmachine` may not start up correctly if `flowdb_testdata`
is still in the start-up phase while the `flowmachine` container is already up and running. This will be fixed soon but for the
time being you can check if there was an error by checking the `flowmachine` logs (via `docker logs -f flowmachine`),
and if necessary running `make flowmachine-down` followed by `make flowmachine-up`)._

To make changes to the containers, you should run `make down && make up`.


### Option 2: Starting Flowmachine and FlowAPI manually, outside the dockerised setup

While the fully dockerised setup described above is convenient, it has the disadvantage that interactive debugging is
difficult or impossible if Flowmachine and FlowAPI are running inside a docker container. If you want to set breakpoints
and use for example [pdb](https://docs.python.org/3/library/pdb.html), [ipdb](https://pypi.org/project/ipdb/) or similar
debuggers to step through the code then it is easier to start the relevant components manually.

To do this, first start the components which you want to run inside the dockerised environment, e.g. via:
```bash
make flowmachine_testdata-up flowauth-up
```

Next, run one or more of the following commands (in separate terminals) to start up the remaining components.

#### Flowmachine
```bash
cd flowmachine/
pipenv run watchmedo auto-restart --recursive --patterns="*.py" --directory="." pipenv run flowmachine
```

#### FlowAPI
```bash
cd flowapi/
pipenv run hypercorn --debug --reload --bind 0.0.0.0:9090 "app.main:create_app()"
```

#### FlowAuth

```bash
cd flowauth/
pipenv run start-all
```

!!! warning
    If you have started FlowMachine and FlowAPI directly, you will need to set the `FLOWKIT_INTEGRATION_TESTS_DISABLE_AUTOSTART_SERVERS=TRUE` before running the integration tests.
    Without this setting, both will be started automatically to allow test coverage to be collected.

## Verifying the setup

This section provides example commands which you can run to verify that `flowdb`, `flowmachine` and `flowapi` started up
successfully and are wired up correctly so that they can talk to each other.

### Running the integration tests

You can run the integration tests as follows. If these pass, this is a good indication that everything is set up correctly.
```bash
cd integration_tests
pipenv install  # only needed once to install dependencies
pipenv run pytest
```

### FlowDB

```
psql "postgresql://flowdb:flowflow@localhost:9000/flowdb" -c "SELECT flowdb_version()"
```
The output should be similar to this:
```
        flowdb_version
-------------------------------
 (0.2.2-8-g0558488,2019-01-21)
(1 row)
```

### Flowmachine

From within the `flowmachine/` folder, run the following command to send an example message to the Flowmachine server via ZeroMQ.
```
pipenv run python -c "from flowmachine.core.server.utils import FM_EXAMPLE_MESSAGE, send_message_and_receive_reply; print(send_message_and_receive_reply(FM_EXAMPLE_MESSAGE))"
```
Expected output:
```
Sending message: {'action': 'run_query', 'query_kind': 'daily_location', 'request_id': 'DUMMY_ID', 'params': {'date': '2016-01-01', 'daily_location_method': 'last', 'aggregation_unit': 'admin3', 'subscriber_subset': 'all'}}
{'status': 'accepted', 'id': 'ddc61a04f608dee16fff0655f91c2057'}
```

### FlowAPI

First navigate to `http://localhost:8080/` (where Flowauth should be running), create a valid token (TODO: add
instructions how to do this or link to the appropriate section in the docs) and store it in the environment
variable `TOKEN`.

Then run the following command to submit a daily location calculation to FlowKit via the API:
```
curl -v -X POST http://localhost:9090/api/0/run \
  -H "Authorization: Bearer ${TOKEN}" \
  -H 'Cache-Control: no-cache' \
  -H 'Content-Type: application/json' \
  -d '{"params": {"date":"2016-01-01", "level":"admin3", "daily_location_method":"last", "aggregation_unit":"admin3", "subscriber_subset":"all"}, "query_kind":"daily_location"}'
```

The output should contain a location header that similar to this:
```
[...]
< location: /api/0/poll/ddc61a04f608dee16fff0655f91c2057
[...]
```