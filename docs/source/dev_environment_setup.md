# Setting up a development environment

## Prerequisites

### Pre-commit hook (for Python code formatting with black)

FlowKit's Python code is formatted with [black](https://black.readthedocs.io/en/stable/) (which is included in the
FlowKit pipenv environments). There is also a [pre-commit](https://pre-commit.com/) hook which runs black on all Python
files before each commit.

To install the pre-commit hook run:
```bash
$ pre-commit install
```
If you ever want to uninstall the hook you can do this by running `pipenv run pre-commit uninstall`.

The above command requires `pre-commit` to be installed on your system. For convenience, it is also
included in the `flowmachine` pipenv environment, so if you don't want to install it system-wide you
can run it as follows (it will still be installed and available for all FlowKit components, not just
flowmachine).
```bash
$ cd flowmachine/
$ pipenv run pre-commit install
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

### Diff tool (for verifying changes in `ApprovalTests`-based tests)

Some of the tests use [ApprovalTests](https://github.com/approvals/ApprovalTests.Python) to verify large output against
a known "approved" version (stored in files called `*approved.txt`). For example, the API specification is
verified in this way. If you make code changes that alter the results of these tests, the content of the
relevant `*.approved.txt` file needs to be updated. ApprovalTests will do this automatically for you, but you need to have a
diff tool installed. (See here for some recommendations for diff tools on [Mac](`https://www.git-tower.com/blog/diff-tools-mac`)
and [Windows](https://www.git-tower.com/blog/diff-tools-windows).)

There is a toplevel config file called `approvaltests_diff_reporters.json` which lists the diff tools that ApprovalTests
will try to find. Currently only `opendiff` is listed there (but we may add additional ones in the future). If you are using a different tool you should
manually add it to this file. See the ApprovalTests [README](https://github.com/approvals/ApprovalTests.Python) for
examples (illustrative particularly for file paths on Windows).


## Option 1: Starting up all FlowKit components inside a dockerised development environment

For convenience, FlowKit comes with a dockerised development environment. You can start up a development version of all
components by running:
```bash
$ make up
```

This will use the specifications in `docker-compose-dev.yml` to spin up development versions of `flowdb_testdata`,
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
384355e94ae6        bitnami/redis                       "/entrypoint.sh /run…"   5 minutes ago       Up 5 minutes        0.0.0.0:6379->6379/tcp             redis_flowkit
```

**Note:** _Currently there is a glitch which means that `flowmachine` may not start up correctly if `flowdb_testdata`
is still in the start-up phase while the `flowmachine` container is already up and running. This will be fixed soon but for the
time being you can check if there was an error by checking the `flowmachine` logs (via `docker logs -f flowmachine`),
and if necessary running `make flowmachine-down` followed by `make flowmachine-up`)._

In this dev setup, the `flowmachine/` and `flowapi/` source folders are mounted as volumes inside the `flowmachine`
and `flowapi` containers. In addition, the `flowmachine` and `flowapi` services are being run in "live reload" mode
(in `flowmachine`'s case this is achieved via [watchdog](https://pythonhosted.org/watchdog/)).
This ensures that any changes to the source code automatically trigger a restart of the process and there is no need to
re-build the images or re-start the docker containers when changes to the code are made during development. (The
only exception is if new dependencies are added, in which case you can run `make down` followed by `make up` to
re-build the images and re-start the containers.)

_(Note: currently this live reload is not enabled for flowauth, but it is planned and will be available very shortly.)_

Any changes to `flowdb` - for example, tweaks to the database schema - will require a re-build and a re-start of the
`flowdb` and/or `flowdb_testdata` image and a re-start of the container. Simply run `make down` followed by `make up`
to achieve this.


## Option 2: Starting Flowmachine and FlowAPI manually, outside the dockerised setup

While the fully dockerised setup described above is convenient, it has the disadvantage that interactive debugging is
difficult or impossible if Flowmachine and FlowAPI are running inside a docker container. If you want to set breakpoints
and use for example [pdb](https://docs.python.org/3/library/pdb.html), [ipdb](https://pypi.org/project/ipdb/) or similar
debuggers to step through the code then it is easier to start the relevant components manually.

To do this, first start the components which you want to run inside the dockerised environment, e.g. via:
```bash
$ make flowmachine_testdata-up flowauth-up
```

Next, run one or more of the following commands (in separate terminals) to start up the remaining components.
(These commands are the same as the ones inside the `Dockerfile-dev` file for each component.) 

### Flowmachine
```bash
$ cd flowmachine/
$ pipenv run watchmedo auto-restart --recursive --patterns="*.py" --directory="." pipenv run flowmachine
```

### FlowAPI
```bash
$ cd flowapi/
$ pipenv run hypercorn --debug --reload --bind 0.0.0.0:9090 "app.main:create_app()"
```

### FlowAuth

_TODO_


# Verifying the setup

This section provides example commands which you can run to verify that `flowdb`, `flowmachine` and `flowapi` started up
successfully and are wired up correctly so that they can talk to each other.

## Running the integration tests

You can run the integration tests as follows. If these pass, this is a good indication that everything is set up correctly.
```bash
$ cd integretion_tests
$ pipenv install  # only needed once to install dependencies
$ pipenv run pytest
```

## FlowDB

```
$ psql "postgresql://flowdb:flowflow@localhost:9000/flowdb" -c "SELECT flowdb_version()"
```
The output should be similar to this:
```
        flowdb_version
-------------------------------
 (0.2.2-8-g0558488,2019-01-21)
(1 row)
```

## Flowmachine

From within the `flowmachine/` folder, run the following command to send an example message to the Flowmachine server via ZeroMQ.
```
$ pipenv run python -c "from flowmachine.core.server.utils import FM_EXAMPLE_MESSAGE, send_message_and_receive_reply; print(send_message_and_receive_reply(FM_EXAMPLE_MESSAGE))"
```
Expected output:
```
Sending message: {'action': 'run_query', 'query_kind': 'daily_location', 'request_id': 'DUMMY_ID', 'params': {'date': '2016-01-01', 'daily_location_method': 'last', 'aggregation_unit': 'admin3', 'subscriber_subset': 'all'}}
{'status': 'accepted', 'id': 'ddc61a04f608dee16fff0655f91c2057'}
```

## FlowAPI

First navigate to `http://localhost:8080/` (where Flowauth should be running), create a valid token (TODO: add
instructions how to do this or link to the appropriate section in the docs) and store it in the environment
variable `TOKEN`.

Then run the following command to submit a daily location calculation to FlowKit via the API:
```
$ curl -v -X POST http://localhost:9090/api/0/run \
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
