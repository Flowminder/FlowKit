# FlowKit

FlowKit is a platform for analysis of CDR and other data. The system is designed to be deployed as a set of [Docker](https://docs.docker.com) containers on a server. The three main server components are:

-   [FlowDB](https://flowminder.github.io/flowkit/flowdb)

    A [PostgreSQL](https://www.postgresql.org) database for storing and serving mobile operator data.

-   [FlowMachine](https://flowminder.github.io/flowkit/flowmachine)

    A Python toolkit for the analysis of CDR data.

-   [FlowAPI](https://flowminder.github.io/flowkit/flowapi)

    An HTTP API which provides access to the functionality of FlowMachine, and handles access control.

In addition, [ZeroMQ](http://zeromq.org/) is used for communication between FlowAPI and FlowMachine, and FlowMachine uses [Redis](https://redis.io/) for interprocess communication. [JSON Web Tokens](http://jwt.io) are used for access control in FlowAPI.

There are two other components of FlowKit:

-   [FlowClient](https://flowminder.github.io/flowkit/flowclient)

    A Python client to FlowAPI.

-   [FlowAuth](https://flowminder.github.io/flowkit/flowauth)

    An authentication management system used to generate access tokens for use with FlowClient.

FlowKit is an open-source product. The source code can be found at https://github.com/Flowminder/flowkit, and documentation is available at https://flowminder.github.io/flowkit.

## Installation using docker cloud repositories

### Server

Docker containers for FlowAPI, FlowMachine and FlowDB are provided in the docker cloud repositories `flowminder/flowapi`, `flowminder/flowmachine` and `flowminder/flowdb`, respectively. To set up FlowKit using the docker cloud repositories, you will need [Docker](https://docs.docker.com/install/) and [Docker Compose](https://docs.docker.com/compose/install/).

The file [`docker-compose.yml`](https://github.com/Flowminder/flowkit/blob/master/docker-compose.yml) can be used to start FlowKit and populate FlowDB with a test dataset. The compose file expects the `JWT_SECRET_KEY` environment variable to be set. This is used to sign and verify access tokens for the API.

The FlowKit test system can be started by running `JWT_SECRET_KEY=<secret> docker-compose up -d` in the same directory as the `docker-compose.yml` file. This will pull any necessary docker containers, and start the system in the background with the API exposed on port `9090` by default.

### Client

The FlowClient Python client may be installed using pip:

```bash
pip install flowclient
```

Details of FlowClient usage can be found [here](https://flowminder.github.io/flowkit/flowclient).

### FlowAuth

The FlowAuth Docker container is provided in the docker cloud repository `flowminder/flowauth`. Deployment and usage instructions can be found [here](https://flowminder.github.io/flowkit/flowauth).

## Installation for developers

After cloning the [GitHub repository](https://github.com/Flowminder/flowkit), the FlowKit system can be started by running `make up` in the root directory. This requires [Docker](https://docs.docker.com/install/) and [Docker Compose](https://docs.docker.com/compose/install/) to be installed, and starts the flowapi, flowmachine, flowdb and redis docker containers using the `docker-compose-dev.yml` file.

FlowKit uses [pipenv](https://pipenv.readthedocs.io/) to manage Python environments. To start a Python session in which you can use FlowClient:

```
cd flowclient
pipenv install
pipenv run python
>>> import flowclient
```

To run the tests in the `flowapi`, `flowclient`, `flowdb`, `flowmachine` or `integration_tests` directory:

```bash
cd <directory>
pipenv install --dev
pipenv run pytest
```

## Running with Secrets

The standard Docker compose file supplies a number of 'secret' values as environment variables. Typically, this is a bad idea.

Instead, you should make use of [docker secrets](https://docs.docker.com/engine/swarm/secrets/), which are stored securely in docker and only made available _inside_ containers. The `secrets_quickstart` directory contains a [docker _stack_](https://docs.docker.com/docker-cloud/apps/stack-yaml-reference/) file (`docker-stack.yml`). The stack file is very similar to a compose file, but removes container names, and adds a new section - secrets.

The stack expects you to provide seven secrets:

-   cert-flowkit.pem

    An SSL certificate file (should contain private key as well)

-   API_DB_USER

    The username the API will use to connect to FlowDB

-   API_DB_PASS

    The password that the API will use to connect to FlowDB

-   FM_DB_USER

    The username that FlowMachine will use to connect to FlowDB

-   FM_DB_PASS

    The password that FlowMachine will use to connect to FlowDB

-   POSTGRES_PASSWORD_FILE

    The superuser password for the `flowdb` user

-   JWT_SECRET_KEY

    The secret key used to sign API access tokens

To make use of secrets you will need to use docker swarm. For testing purposes, you can set up a single node swarm by running `docker swarm init`.

Once you have created a swarm, you can add secrets to it using the [docker secret](https://docs.docker.com/engine/reference/commandline/secret_create/) command. For example, to add a randomly generated password for the `FM_DB_PASS` secret:

```bash
openssl rand -base64 16 | docker secret create FM_DB_PASS -
```

And to add the (unsigned) localhost SSL certificate supplied in the `integration_tests` directory:

```bash
docker secret create cert-flowkit.pem integration_tests/cert.pem
```

(Note that unlike the other examples, we are supplying a _file_ rather than piping to stdin.)

Once you have added all five required secrets, you can use `docker stack` to spin up FlowKit, much as you would `docker-compose`:

```bash
cd secrets_quickstart
docker stack deploy --with-registry-auth -c docker-stack.yml secrets_test
```

After which, the API will be available via HTTPS (and no longer available via HTTP). Note that to access the API using FlowClient, you'll need to provide the path to the certificate as the `verify` argument when calling `flowclient.Connection` (much as you would if using a self-signed certificate with `requests`):

```python
import flowclient
conn = flowclient.Connection("https://localhost:9090", "JWT_STRING", verify="/home/username/flowkit/integration_tests/client_cert.pem")
```

### Secrets Quickstart

```bash
cd secrets_quickstart
docker login
docker swarm init
openssl rand -base64 16 | docker secret create FM_DB_PASS -
echo "fm" | docker secret create FM_DB_USER -
echo "api" | docker secret create API_DB_USER -
openssl rand -base64 16 | docker secret create API_DB_PASS -
openssl rand -base64 16 | docker secret create POSTGRES_PASSWORD_FILE -
openssl req -newkey rsa:4096 -days 3650 -nodes -x509 -subj "/CN=flow.api" \
    -extensions SAN \
    -config <( cat $( [[ "Darwin" -eq "$(uname -s)" ]]  && echo /System/Library/OpenSSL/openssl.cnf || echo /etc/ssl/openssl.cnf  ) \
    <(printf "[SAN]\nsubjectAltName='DNS.1:localhost,DNS.2:flow.api'")) \
    -keyout cert.key -out cert.pem
cat client_cert.key cert.pem > cert-flowkit.pem
docker secret create cert-flowkit.pem cert-flowkit.pem
echo "secret" | docker secret create JWT_SECRET_KEY -
docker stack deploy --with-registry-auth -c docker-stack.yml secrets_test
```

This will bring up a single node swarm, create random 16 character passwords for the database users, generate a certificate valid for the `flowkit.api` domain (and point that to `localhost` using `/etc/hosts`), pull all necessary containers, and bring up the API with `secret` as the JWT secret key.

For convenience, you can also do `pipenv run secrets_quickstart` from the `secrets_quickstart` directory.

Note that if you wish to deploy a branch other than master, you should set the `CIRCLE_BRANCH` environment variable before running, to ensure that Docker pulls the correct tags.

You can then provide the certificate to `flowclient`, and finally connect via https:

```python
import flowclient
conn = flowclient.Connection("https://localhost:9090", "JWT_STRING", verify="<path_to_cert.pem>")
```

(This generates a certificate valid for the `flow.api` domain as well, which you can use by adding a corresponding entry to your `/etc/hosts` file.)
