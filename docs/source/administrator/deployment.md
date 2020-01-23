Title: Deploying FlowKit

A complete FlowKit deployment consists of FlowDB, FlowMachine, FlowETL, FlowAPI, FlowAuth, and redis. FlowDB, FlowMachine, FlowETL, FlowAPI and redis are deployed inside your firewall and work together to provide a complete running system. FlowAuth can be installed _outside_ your firewall, and does not require a direct connection to the rest of the system.

We strongly recommend using [docker swarm](https://docs.docker.com/engine/swarm/) to deploy all the components, to support you in safely managing [secrets](https://docs.docker.com/engine/swarm/secrets/) to ensure a secure system. Ensure you understand how to [create a swarm](https://docs.docker.com/engine/reference/commandline/swarm_init/), [manage secrets](https://docs.docker.com/engine/swarm/secrets/#read-more-about-docker-secret-commands), and [deploy stacks using compose files](https://docs.docker.com/engine/reference/commandline/stack_deploy/#compose-file) before continuing.

## Deployment scenarios

### FlowDB and FlowETL only

FlowDB can be used with FlowDB independently of the other components, to provide a system which allows access to individual level data via a harmonised schema and SQL access. Because FlowDB is built on PostgreSQL, standard SQL based tools and workflows will work just fine.

#### Caveats

##### Shared memory

You will typically need to increase the default shared memory available to docker containers when running FlowDB. You can do this either by setting `shm_size` for the FlowDB container in your compose or stack file, or by passing the `--shm-size` argument to the `docker run` command.

##### Bind Mounts and user permissions

By default, FlowDB will create and attach a docker volume that contains all data. In some cases, this will be sufficient for use.

However, you will often wish to set up bind mounts to hold the data and allow FlowDB to consume new data. To avoid sticky situations with permissions, you will want to specify the uid and gid that FlowDB runs with to match an existing user on the host system.

Adding a bind mount using `docker-compose` is simple:

```yaml
services:
    flowdb:
    ...
        user: HOST_USER_ID:HOST_GROUP_ID
        volumes:
          - /path/to/store/data/on/host:/var/lib/postgresql/data
          - /path/to/consume/data/from/host:/etl:ro
```

This creates two bind mounts, the first is FlowDB's internal storage, and the second is a *read only* mount for loading new data. The user FlowDB runs as inside the container will also be changed to the uid specified.

!!! warning
    If the bind mounted directories do not exist, docker will create them and you will need to `chown` them to the correct user.

And similarly when using `docker run`:

```bash
docker run --name flowdb_testdata -e FLOWMACHINE_FLOWDB_PASSWORD=foo -e FLOWAPI_FLOWDB_PASSWORD=foo \
 --publish 9000:5432 \
 --user HOST_USER_ID:HOST_GROUP_ID \
 -v /path/to/store/data/on/host:/var/lib/postgresql/data \
 -v /path/to/consume/data/from/host:/etl:ro \
 --detach flowminder/flowdb-testdata:latest
```

!!! tip
    To run as the current user, you can simply replace `HOST_USER_ID:HOST_GROUP_ID` with `$(id -u):$(id -g)`.


!!! warning
    Using the `--user` flag without a bind mount specified will not work, and you will see an error
    like this: `initdb: could not change permissions of directory "/var/lib/postgresql/data": Operation not permitted`.

    When using docker volumes, docker will manage the permissions for you.

### FlowDB, FlowETL and FlowMachine

For cases where your users require individual level data access, you support the use of FlowMachine as a library. In this mode, users connect directly to FlowDB via the FlowMachine Python module. Many of the benefits of a complete FlowKit deployment are available in this scenario, including query caching.

### FlowKit

A complete FlowKit deployment makes aggregated insights easily available to end users via a web API and FlowClient, while allowing administrators granular control over who can access what data, and for how long.

To deploy a complete FlowKit system, you will first need to generate a key pair which will be used to connect FlowAuth and FlowAPI. (If you have an existing FlowAuth deployment, you do not need to generate a new key pair - you can use the _same_ public key with all the FlowAPI servers managed by that FlowAuth server).

#### Generating a key pair

FlowAuth uses a private key to sign the tokens it generates, which ensures that they can be trusted by FlowAPI. When deploying instances of FlowAPI, you will need to supply them with the corresponding _public_ key, to allow them to verify the tokens were produced by the right instance of FlowAuth.

You can use `openssl` to generate a private key:

```bash
openssl genrsa -out flowauth-private-key.key 4096
```

And then create a public key from the key file (`openssl rsa -pubout -in flowauth-private-key.key -out flowapi-public-key.pub`). Should you need to supply the key using environment variables, rather than secrets (not recommended), you should base64 encode the key (e.g. `base64 -i flowauth-private-key.key`). FlowAuth and FlowAPI will automatically decode base64 encoded keys for use.

!!!warning 
    _Always keep your private key secure_ 

    If your key is destroyed, you will need to generate a new one and redeploy any instances of FlowAuth and FlowAPI. If you key is _leaked_, unauthorised parties will be able to sign tokens for your instances of FlowAPI. 

#### FlowAuth Production Deployment

FlowAuth is designed to be deployed as a single Docker container working in cooperation with a database and, typically, an ssl reverse proxy (e.g. [nginx-proxy](https://github.com/jwilder/nginx-proxy) combined with [letsencrypt-nginx-proxy-companion](https://github.com/JrCs/docker-letsencrypt-nginx-proxy-companion)).  

FlowAuth supports any database supported by [SQLAlchemy](https://sqlalche.me), and to connect you will only need to supply a correct URI for the database either using the `DB_URI` environment variable, or by setting the `DB_URI` secret. If `DB_URI` is not set, a temporary sqlite database will be created. For database backends which require a username and password, you may supply the URI with `{}` in place of the password and provide the password using the `FLOWAUTH_DB_PASSWORD` secret or environment variable.

FlowAuth will attempt to create all necessary tables when first accessed, but will not overwrite any existing tables. To wipe any existing data, you can either set the `RESET_FLOWAUTH_DB` environment variable to `true`, or run `flask init-db` from inside the container (`docker exec <container-id> flask init-db`).

FlowAuth requires you to create at least one admin user by setting the `FLOWAUTH_ADMIN_USER` and `FLOWAUTH_ADMIN_PASSWORD` environment variables or providing them as secrets. You can combine these environment variables with the `RESET_FLOWAUTH_DB` environment variable. If the user already exists, their password will be reset to the provided value and they will be promoted to admin.

You _must_ also provide three additional environment variables or secrets: `FLOWAUTH_FERNET_KEY`, `SECRET_KEY`, and `PRIVATE_JWT_SIGNING_KEY`. `FLOWAUTH_FERNET_KEY` is used to encrypt tokens while 'at rest' in the database, and decrypt them for use. `SECRET_KEY` is used to secure session and CSRF protection cookies.

`PRIVATE_JWT_SIGNING_KEY` is used to sign the tokens generated by FlowAuth, which ensures that they can be trusted by FlowAPI. When deploying instances of FlowAPI, you will need to supply them with the corresponding _public_ key, to allow them to verify the tokens were produced by this instance of FlowAuth.

By default `SECRET_KEY` may be any arbitrary string, `FLOWAUTH_FERNET_KEY` should be a valid Fernet key.

!!!note 
    Generating Fernet keys
    
    A convenient way to generate Fernet keys is to use the python [cryptography](https://cryptography.io/) package. After installing, you can generate a new key by running `python -c "from cryptography.fernet import Fernet;print(Fernet.generate_key().decode())"`.

##### Two-factor authentication

FlowAuth supports optional two-factor authentication for user accounts, using the Google Authenticator app or similar. This can be enabled either by an administrator, or by individual users.

To safeguard two-factor codes, FlowAuth prevents users from authenticating more than once with the same code within a short window. When deploying to production, you may wish to deploy a redis backend to support this feature - for example if you are deploying multiple instances of the FlowAuth container which need to be able to record the last used codes for users in a common place.

To configure FlowAuth for use with redis, set the `FLOWAUTH_CACHE_BACKEND` environment variable to `redis`. You will also need to set the following environment variables, or docker secrets:

- `FLOWAUTH_REDIS_HOST`
   
   The hostname to connect to redis on.
- `FLOWAUTH_REDIS_PORT`
    
    The port to use to connect to redis, defaults to `6379`.
- `FLOWAUTH_REDIS_PASSWORD`
    
    The password for the redis database.
- `FLOWAUTH_REDIS_DB`
    
    The database _number_ to connect to, defaults to `0`.
    
By default, FlowAuth will use a dbm file backend to track last used two-factor codes. This file will be created at `/dev/shm/flowauth_last_used_cache` inside the container (i.e. in Docker's shared memory area), and can be mounted to a volume or pointed to an alternative location by setting the  `FLOWAUTH_CACHE_FILE` environment variable.

##### Sample stack files

You can find an example docker stack file for FlowAuth [here](https://github.com/Flowminder/FlowKit/blob/master/flowauth/docker-compose.yml). This will bring up instances of FlowAuth, redis, and postgres. You can combine this with the [letsencrypt](https://github.com/Flowminder/FlowKit/blob/master/flowauth/docker-compose.letsencrypt.yml) stack file to automatically acquire an SSL certificate.

#### FlowMachine and FlowAPI

Once you have FlowAuth, FlowDB, and FlowETL running, you are ready to add FlowMachine and FlowAPI.

#### Running with Secrets

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

#### Flow

##### Secrets Quickstart

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

docker stack deploy -c docker-stack.yml -c ../flowauth/docker-compose.yml secrets_test

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



#### AutoFlow Production Deployment

For a production deployment, you would typically want to deploy AutoFlow with a separate database, instead of the default SQLite database created within the AutoFlow container. It is also advisable to set `AUTOFLOW_DB_PASSWORD` and `FLOWAPI_TOKEN` as docker secrets. An example Docker stack file for deploying AutoFlow in this way is provided at https://raw.githubusercontent.com/Flowminder/FlowKit/master/autoflow/docker-stack.yml.

#### Demonstrating successful deployment

Once FlowKit installation is complete, we suggest running the provided [worked examples](analyst/worked_examples/index.md) against the deployed FlowKit to check that everything is working correctly.

## Additional support

If you require more assistance to get up and running, please reach out to us by [email](mailto:flowkit@flowminder.org) and we will try to assist.
