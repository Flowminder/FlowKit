Title: Installation

# How to Install FlowKit

There are three main ways to install FlowKit.

- [Quick Install](#quickinstall); suitable for a try-out on a local PC, includes basic example using FlowClient.
- [Developer Install](#developers); for those wishing to contribute code
- [Production Install](#prodinstall); for deployment, e.g. inside an MNO firewall

<a name="installationrequirements">

## Installation Requirements

Most FlowKit components (FlowDB, FlowMachine, FlowAPI, FlowAuth) are distributed as docker containers. To install these, you need:

- `docker >= 17.12.0`
- `docker-compose >= 1.21.0`

In addition, running FlowClient requires:

- `Python >= 3.6`

There are additional requirements for a development setup. See the [Developer install](#developers) section below for details.

<a name="quickinstall">

## Quick Install

This quick install guide will install the major components of FlowKit together with an intial setup and example analysis query.

The bulk of the installation process consists of using [Docker Compose](https://docs.docker.com/compose/) to download [Docker](https://docs.docker.com/install/) containers from [Docker Hub](https://hub.docker.com/u/flowminder), followed by a `pip install` of FlowClient.

These instructions assume use of [Pyenv](https://github.com/pyenv/pyenv) and [Pipenv](https://github.com/pypa/pipenv). If you are using [Anaconda](https://www.anaconda.com/what-is-anaconda/)-based installation commands may be different.

Docker containers for FlowAPI, FlowMachine, FlowDB, FlowAuth and the worked examples are provided in the Docker Hub repositories [flowminder/flowapi](https://hub.docker.com/r/flowminder/flowapi), [flowminder/flowmachine](https://hub.docker.com/r/flowminder/flowmachine), [flowminder/flowdb](https://hub.docker.com/r/flowminder/flowdb), [flowminder/flowauth](https://hub.docker.com/r/flowminder/flowauth), and [flowminder/flowkit-examples](https://hub.docker.com/r/flowminder/flowkit-examples) respectively. To install them, you will need [Docker](https://docs.docker.com/install/) and [Docker Compose](https://docs.docker.com/compose/install/).

Start the FlowKit test system by running

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
The small dataset is sufficient for most of the worked examples, but the larger dataset is required for the [Flows Above Normal](worked_examples/flows-above-normal.ipynb) example because this uses data for dates outside the range included in the small dataset.

!!! info
The worked examples make use of [Mapbox GL](https://mapbox-mapboxgl-jupyter.readthedocs-hosted.com/en/latest/) for visualisation, which requires an API access token. If you would like to produce the maps in the worked examples notebooks, you will need to create a mapbox access token (following instructions [here](https://account.mapbox.com/)), and set this as the value of the `MAPBOX_ACCESS_TOKEN` environment variable before running the above commands.

To shut down the system, you can either stop all the docker containers directly, or run

```bash
bash <(curl -s https://raw.githubusercontent.com/Flowminder/FlowKit/master/quick_start.sh) stop
```

In order to use the test system, now [install FlowClient](#flowclient), and generate a token using FlowAuth.

### FlowAuth Quickstart

Visit <a href="http://localhost:9091/" target="_blank">http://localhost:9091</a> and log in with either `TEST_ADMIN:DUMMY_PASSWORD` or `TEST_USER:DUMMY_PASSWORD`. `TEST_USER` is already set up to generate tokens for the FlowAPI instance started by the quick start script.

See the [administrator section](administrator.md#granting-user-permissions-in-flowauth) for details of how to add servers and users or modify user permissions, or the [analyst section](analyst.md#flowauth) for instructions to generate a token.

### FlowClient <a name="flowclient"> </a>

The FlowClient Python client is used to perform CDR analysis using the JupyterLab Python Data Science Stack. It may be installed using pip:

```bash
pip install flowclient
```

Quick install is continued with an example of FlowClient usage [here](analyst.md#flowclient).

<a name="developers">

## Developer Install</a>

### Installation Requirements for Developers

Just as for a regular installation, you will need `docker` and `docker-compose` (see [Installation requirements](#installationrequirements) above).

During development, you will typically also want to run FlowMachine, FlowAPI and FlowAuth outside docker containers. This requires additional prerequisites to be available.

- [Pipenv](https://pipenv.readthedocs.io/en/latest/) (to manage separate pipenv environment for each FlowKit component)
- FlowMachine server: `Python >= 3.7`
- FlowAuth: `npm` (we recommend installing it via [nvm](https://github.com/nvm-sh/nvm)); [Cypress](https://www.cypress.io/) for testing

### Setting up FlowKit for development

After cloning the [GitHub repository](https://github.com/Flowminder/FlowKit), the FlowKit system can be started by running `make up` in the root directory. This requires Docker](https://docs.docker.com/install/) and [Docker Compose](https://docs.docker.com/compose/install/) to be installed, and starts the flowapi, flowmachine, flowdb and redis docker containers using the `docker-compose.yml` file.

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

Also see [Setting up a development environment](developer/dev_environment_setup.md) for further details on setting up FlowKit for code development.

 <a name="prodinstall">

## Production Install

Contact Flowminder on [flowkit@flowminder.org](mailto:flowkit@flowminder.org) for full instructions. Instructions on FlowAuth production deployment and dealing with docker secrets is described below. Note that these instructions are likely subject to change.

### FlowAuth Production Deployment

FlowAuth is designed to be deployed as a single Docker container working in cooperation with a database and, typically, an ssl reverse proxy (e.g. [nginx-proxy](https://github.com/jwilder/nginx-proxy) combined with [letsencrypt-nginx-proxy-companion](https://github.com/JrCs/docker-letsencrypt-nginx-proxy-companion)).

FlowAuth supports any database supported by [SQLAlchemy](https://sqlalche.me), and to connect you will only need to supply a correct URI for the database either using the `DB_URI` environment variable, or by setting the `DB_URI` secret. If `DB_URI` is not set, a temporary sqlite database will be created. For database backends which require a username and password, you may supply the URI with `{}` in place of the password and provide the password using the `FLOWAUTH_DB_PASSWORD` secret or environment variable.

FlowAuth will attempt to create all necessary tables when first accessed, but will not overwrite any existing tables. To wipe any existing data, you can either set the `RESET_FLOWAUTH_DB` environment variable to `true`, or run `flask init-db` from inside the container (`docker exec <container-id> flask init-db`).

FlowAuth requires you to create at least one admin user by setting the `FLOWAUTH_ADMIN_USER` and `FLOWAUTH_ADMIN_PASSWORD` environment variables or providing them as secrets. You can combine these environment variables with the `INIT_DB` environment variable. If the user already exists, their password will be reset to the provided value and they will be promoted to admin.

You _must_ also provide three additional environment variables or secrets: `FLOWAUTH_FERNET_KEY`, `SECRET_KEY`, and `PRIVATE_JWT_SIGNING_KEY`. `FLOWAUTH_FERNET_KEY` is used to encrypt tokens while 'at rest' in the database, and decrypt them for use. `SECRET_KEY` is used to secure session and CSRF protection cookies.

`PRIVATE_JWT_SIGNING_KEY` is used to sign the tokens generated by FlowAuth, which ensures that they can be trusted by FlowAPI. When deploying instances of FlowAPI, you will need to supply them with the corresponding _public_ key, to allow them to verify the tokens were produced by this instance of FlowAuth.

You can use `openssl` to generate a private key:

```bash
openssl genrsa -out flowauth-private-key.key 4096
```

And then create a public key from the key file (`openssl rsa -pubout -in flowauth-private-key.key -out flowapi-public-key.pub`), or download it from FlowAuth once started. Should you need to supply the key using environment variables, rather than secrets (not recommended), you should base64 encode the key (e.g. `base64 -i flowauth-private-key.key`). FlowAuth and FlowAPI will automatically decode base64 encoded keys for use.

By default `SECRET_KEY` may be any arbitrary string, `FLOWAUTH_FERNET_KEY` should be a valid Fernet key. A convenience command is provided to generate one - `flask get-fernet`.

### Running with Secrets

The standard Docker compose file supplies a number of 'secret' values as environment variables. Typically, this is a bad idea.

Instead, you should make use of [docker secrets](https://docs.docker.com/engine/swarm/secrets/), which are stored securely in docker and only made available _inside_ containers. The `secrets_quickstart` directory contains a docker _stack_ file (`docker-stack.yml`). The stack file is very similar to a compose file, but removes container names, and adds a new section - [secrets](https://docs.docker.com/compose/compose-file/#secrets-configuration-reference).

The stack expects you to provide fifteen secrets:

- cert-flowkit.pem

  An SSL certificate file (should contain private key as well)

- FLOWAPI_FLOWDB_USER

  The username the API will use to connect to FlowDB

- FLOWAPI_FLOWDB_PASSWORD

  The password that the API will use to connect to FlowDB

- FLOWMACHINE_FLOWDB_USER

  The username that FlowMachine will use to connect to FlowDB

- FLOWMACHINE_FLOWDB_PASSWORD

  The password that FlowMachine will use to connect to FlowDB

- POSTGRES_PASSWORD

  The superuser password for the `flowdb` user

- REDIS_PASSWORD

  The password for redis

- PRIVATE_JWT_SIGNING_KEY

  The secret key used to sign API access tokens, should be an RSA private key

- PUBLIC_JWT_SIGNING_KEY

  The public key used to verify API access tokens, should be the public key which matches PRIVATE_JWT_SIGNING_KEY

- FLOWAPI_IDENTIFIER

  The unique name of the FlowAPI server. Used to verify that a decoded token is intended for _this_ server.
  
-  FLOWAUTH_FERNET_KEY
    
   Used for encrypted-at-rest storage of tokens in flowauth
    
-  FLOWAUTH_ADMIN_USERNAME

   Default flowauth administrator username
    
-  FLOWAUTH_ADMIN_PASSWORD

   Default flowauth administrator password

-  FLOWAUTH_DB_PASSWORD

   Password for flowauth's database
    
-  SECRET_KEY

    Secret key for Flowauth's Flask, used for signing cookies

To make use of secrets you will need to use docker swarm. For testing purposes, you can set up a single node swarm by running `docker swarm init`.

Once you have created a swarm, you can add secrets to it using the [docker secret](https://docs.docker.com/engine/reference/commandline/secret_create/) command. For example, to add a randomly generated password for the `FLOWMACHINE_FLOWDB_PASSWORD` secret:

```bash
openssl rand -base64 16 | docker secret create FLOWMACHINE_FLOWDB_PASSWORD -
```

And to add the (unsigned) localhost SSL certificate supplied in the `integration_tests` directory:

```bash
docker secret create cert-flowkit.pem integration_tests/cert.pem
```

(Note that unlike the other examples, we are supplying a _file_ rather than piping to stdin.)

Once you have added all the required secrets, you can use `docker stack` to spin up FlowKit, much as you would `docker-compose`:

```bash
cd secrets_quickstart
docker stack deploy -c docker-stack.yml secrets_test
```

After which, the API will be available via HTTPS (and no longer available via HTTP). Note that to access the API using FlowClient, you'll need to provide the path to the certificate as the `ssl_certificate` argument when calling `flowclient.Connection` (much as you would if using a self-signed certificate with `requests`):

```python
import flowclient
conn = flowclient.Connection("https://localhost:9090", "JWT_STRING", ssl_certificate="/home/username/flowkit/integration_tests/client_cert.pem")
```

#### Secrets Quickstart

```bash
#!/usr/bin/env bash
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

set -eu
set -o pipefail

# Ensure this docker node is in swarm mode
docker swarm init || true


# Remove existing stack deployment
echo "Removing existing secrets_test_stack"
docker stack rm secrets_test

# Wait for 'docker stack rm' to finish (see https://github.com/moby/moby/issues/30942)
limit=30
until [ -z "$(docker service ls --filter label=com.docker.stack.namespace=secrets_test -q)" ] || [ "$limit" -lt 0 ]; do
  sleep 2
  limit="$((limit-1))"
done
if [ "$limit" -lt 0 ]; then
    echo "Not all services in the existing docker stack have been removed."
    echo "Please wait (or try to remove them manually) and run this script again."
    exit 1
fi

limit=30
until [ -z "$(docker network ls --filter label=com.docker.stack.namespace=secrets_test -q)" ] || [ "$limit" -lt 0 ]; do
  sleep 2
  limit="$((limit-1))"
done
if [ "$limit" -lt 0 ]; then
    echo "Not all networks in the existing docker stack have been removed."
    echo "Please wait (or try to remove them manually) and run this script again."
    exit 1
fi


# Remove existing secrets
echo "Removing existing secrets"

docker secret rm FLOWMACHINE_FLOWDB_PASSWORD || true
docker secret rm FLOWMACHINE_FLOWDB_USER || true
docker secret rm FLOWAPI_FLOWDB_PASSWORD || true
docker secret rm FLOWAPI_FLOWDB_USER || true
docker secret rm FLOWDB_POSTGRES_PASSWORD || true
docker secret rm cert-flowkit.pem || true
docker secret rm REDIS_PASSWORD || true
docker secret rm FLOWAPI_IDENTIFIER || true
docker secret rm FLOWAUTH_ADMIN_USERNAME || true
docker secret rm FLOWAUTH_ADMIN_PASSWORD || true
docker secret rm FLOWAUTH_DB_PASSWORD || true
docker secret rm PRIVATE_JWT_SIGNING_KEY || true
docker secret rm PUBLIC_JWT_SIGNING_KEY || true
docker secret rm FLOWAUTH_FERNET_KEY || true
docker secret rm SECRET_KEY || true

# Add new secrets

echo "Adding secrets"
# Flowauth
openssl genrsa -out tokens-private-key.key 4096
FLOWAUTH_DB_PASSWORD=$(openssl rand -base64 16 | tr -cd '0-9-a-z-A-Z')
echo "$FLOWAUTH_DB_PASSWORD" | docker secret create FLOWAUTH_DB_PASSWORD -
FLOWAUTH_ADMIN_PASSWORD=$(openssl rand -base64 16 | tr -cd '0-9-a-z-A-Z')
echo "$FLOWAUTH_ADMIN_PASSWORD"| docker secret create FLOWAUTH_ADMIN_PASSWORD -
SECRET_KEY=$(openssl rand -base64 64)
echo "$SECRET_KEY"| docker secret create SECRET_KEY -
echo "admin" | docker secret create FLOWAUTH_ADMIN_USERNAME -
pip install cryptography
FLOWAUTH_FERNET_KEY=$(python -c "from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())")
echo "$FLOWAUTH_FERNET_KEY"| docker secret create FLOWAUTH_FERNET_KEY -


# Flowmachine
FLOWMACHINE_FLOWDB_PASSWORD=$(openssl rand -base64 16 | tr -cd '0-9-a-z-A-Z')
echo "$FLOWMACHINE_FLOWDB_PASSWORD" | docker secret create FLOWMACHINE_FLOWDB_PASSWORD -
echo "flowmachine" | docker secret create FLOWMACHINE_FLOWDB_USER -
echo "flowapi" | docker secret create FLOWAPI_FLOWDB_USER -
REDIS_PASSWORD=$(openssl rand -base64 16 | tr -cd '0-9-a-z-A-Z')
echo "$REDIS_PASSWORD"| docker secret create REDIS_PASSWORD -

# FlowDB
FLOWAPI_FLOWDB_PASSWORD=$(openssl rand -base64 16 | tr -cd '0-9-a-z-A-Z')
echo "$FLOWAPI_FLOWDB_PASSWORD" | docker secret create FLOWAPI_FLOWDB_PASSWORD -
FLOWDB_POSTGRES_PASSWORD=$(openssl rand -base64 16 | tr -cd '0-9-a-z-A-Z')
echo "$FLOWDB_POSTGRES_PASSWORD" | docker secret create FLOWDB_POSTGRES_PASSWORD -

# FlowAPI
openssl rsa -pubout -in  tokens-private-key.key -out tokens-public-key.pub
docker secret create PRIVATE_JWT_SIGNING_KEY tokens-private-key.key
docker secret create PUBLIC_JWT_SIGNING_KEY tokens-public-key.pub
echo "flowapi_server" | docker secret create FLOWAPI_IDENTIFIER -
openssl req -newkey rsa:4096 -days 3650 -nodes -x509 -subj "/CN=flow.api" \
    -extensions SAN \
    -config <( \
        cat $(   ( [[ -e /System/Library/OpenSSL/openssl.cnf ]] \
              && echo "/System/Library/OpenSSL/openssl.cnf" ) \
              || ( [[ -e /etc/ssl/openssl.cnf ]] && echo "/etc/ssl/openssl.cnf" ) \
              || ( [[ -e /etc/pki/tls/openssl.cnf ]] && echo "/etc/pki/tls/openssl.cnf" ) ) \
    <(printf "[SAN]\nsubjectAltName='DNS.1:localhost,DNS.2:flow.api'")) \
    -keyout cert.key -out cert.pem
if ! ( [ -e cert.key ] && [ -e cert.pem ] ); then
    echo "Generation of the SSL certificate failed."
    echo "Please the check the command (in particular the path to the openssl.cnf file) and try again."
    exit
fi
cat cert.key cert.pem > cert-flowkit.pem
docker secret create cert-flowkit.pem cert-flowkit.pem

# Deploy the stack

echo "
===============

Deploying stack
"

docker stack deploy -c docker-stack.yml secrets_test

echo "

===============

Deployed with settings:

FlowDB users
flowmachine:$FLOWMACHINE_FLOWDB_PASSWORD
flowapi:$FLOWAPI_FLOWDB_PASSWORD
flowdb:$FLOWDB_POSTGRES_PASSWORD

FlowAuth admin user
admin:$FLOWAUTH_ADMIN_PASSWORD

FlowAuth DB user
flowauth:$FLOWAUTH_DB_PASSWORD

FlowAuth Fernet Key
$FLOWAUTH_FERNET_KEY

FlowAuth secret Key
$SECRET_KEY

Redis password
$REDIS_PASSWORD

===============
"



```

This will bring up a single node swarm, create random 16 character passwords for the database users, generate a fresh RSA key pair which links FlowAuth and FlowAPI, generate a certificate valid for the `flowkit.api` domain (and point that to `localhost` using `/etc/hosts`), pull all necessary containers, and bring up FlowAuth and FlowAPI.

For convenience, you can also do `pipenv run secrets_quickstart` from the `secrets_quickstart` directory.

Note that if you wish to deploy a branch other than `master`, you should set the `CONTAINER_TAG` environment variable before running, to ensure that Docker pulls the correct tags.

You can then provide the certificate to `flowclient`, and finally connect via https:

```python
import flowclient
conn = flowclient.Connection(url="https://localhost:9090", token="JWT_STRING", ssl_certificate="<path_to_cert.pem>")
```

(This generates a certificate valid for the `flow.api` domain as well, which you can use by adding a corresponding entry to your `/etc/hosts` file.)

### Demonstrating successful deployment

Once FlowKit installation is complete, we suggest running the provided [worked examples](worked_examples/index.md) against the deployed FlowKit to check that everything is working correctly.
