Title: Deploying FlowKit

A complete FlowKit deployment consists of FlowDB, FlowMachine, FlowETL, FlowAPI, FlowAuth, and redis. FlowDB, FlowMachine, FlowETL, FlowAPI and redis are deployed inside your firewall and work together to provide a complete running system. FlowAuth can be installed _outside_ your firewall, and does not require a direct connection to the rest of the system.

We strongly recommend using [docker swarm](https://docs.docker.com/engine/swarm/) to deploy all the components, to support you in safely managing [secrets](https://docs.docker.com/engine/swarm/secrets/) to ensure a secure system. Ensure you understand how to [create a swarm](https://docs.docker.com/engine/reference/commandline/swarm_init/), [manage secrets](https://docs.docker.com/engine/swarm/secrets/#read-more-about-docker-secret-commands), and [deploy stacks using compose files](https://docs.docker.com/engine/reference/commandline/stack_deploy/#compose-file) before continuing.

## Deployment scenarios

!!! warning
    If updating from FlowKit v. 1.16 or earlier, it is **strongly recommended** that you take a backup of the flowetl_db database. Airflow has been bumped to v2 from v1.

### FlowDB and FlowETL only

FlowDB can be used with FlowETL independently of the other components, to provide a system which allows access to individual level data via a harmonised schema and SQL access. Because FlowDB is built on PostgreSQL, standard SQL based tools and workflows will work just fine.

#### FlowDB

FlowDB is distributed as a docker container. To run it, you will need to provide several secrets:

| Secret name | Secret purpose | Notes |
| ----------- | -------------- | ----- |
| FLOWAPI_FLOWDB_USER | Database user used by FlowAPI | Role with _read_ access to tables under the cache and geography schemas | 
| FLOWAPI_FLOWDB_PASSWORD | Password for the FlowAPI database user | |
| FLOWMACHINE_FLOWDB_USER | Database user for FlowMachine | Role with _write_ access to tables under the cache schema, and _read_ access to events, infrastructure, cache and geography schemas |
| FLOWMACHINE_FLOWDB_PASSWORD | Password for flowmachine user | |
| FLOWDB_POSTGRES_PASSWORD | Postgres superuser password for flowdb | Username `flowdb`, user with super user access to flowdb database |

You may also provide the following environment variables:

| Variable name | Purpose | Default value |
| ------------- | ------- | ------------- |
| CACHE_SIZE | Maximum size of the cache schema | 1 tenth of available space in pgdata directory |
| CACHE_PROTECTED_PERIOD | Amount of time to protect cache tables from being cleaned up | 86400 (24 hours) |
| CACHE_HALF_LIFE | Speed at which cache tables expire when not used | 1000 |
| MAX_CPUS | Maximum number of CPUs that may be used for parallelising queries | The greater of 1 or 1 less than all CPUs |
| SHARED_BUFFERS_SIZE | Size of shared buffers | 16GB |
| MAX_WORKERS |  Maximum number of CPUs that may be used for parallelising one query | MAX_CPUS/2 |
| MAX_WORKERS_PER_GATHER |  Maximum number of CPUs that may be used for parallelising part of one query | MAX_CPUS/2 |
| EFFECTIVE_CACHE_SIZE | Postgres cache size | 25% of total RAM |
| FLOWDB_ENABLE_POSTGRES_DEBUG_MODE | When set to TRUE, enables use of the [pgadmin debugger](https://www.pgadmin.org/docs/pgadmin4/4.13/debugger.html) | FALSE |

However in most cases, the defaults will be adequate.

##### Shared memory

You will typically need to increase the default shared memory available to docker containers when running FlowDB. You can do this either by setting `shm_size` for the FlowDB container in your compose or stack file, or by passing the `--shm-size` argument to the `docker run` command.

##### Bind Mounts and user permissions

By default, FlowDB will create and attach a docker volume that contains all data. In some cases, this will be sufficient for use.

However, you will often wish to set up bind mounts to hold the data, allow access to the postgres logs, and allow FlowDB to consume new data. To avoid sticky situations with permissions, you will want to specify the uid and gid that FlowDB runs with to match an existing user on the host system.

Adding a bind mount using `docker-compose` is simple:

```yaml
services:
    flowdb:
    ...
        user: <HOST_USER_ID>:<HOST_GROUP_ID>
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

#### FlowETL

To run FlowETL, you will need to provide the following secrets:

| Secret name | Secret purpose | Notes |
| ----------- | -------------- | ----- |
| FLOWETL_AIRFLOW_ADMIN_USERNAME | Default administrative user logon name for the FlowETL web interface | |
| FLOWETL_AIRFLOW_ADMIN_PASSWORD | Password for the administrative user | |
| AIRFLOW__CORE__SQL_ALCHEMY_CONN | Connection string for the backing database | Should take the form `postgres://flowetl:<FLOWETL_POSTGRES_PASSWORD>@flowetl_db:5432/flowetl` |
| AIRFLOW__CORE__FERNET_KEY | Ferney key used to encrypt (at rest) database credentials | |
| AIRFLOW_CONN_FLOWDB | Connection string for the FlowDB database | Should take the form `postgres://flowdb:<FLOWDB_POSTGRES_PASSWORD>@flowdb:5432/flowdb` |
| FLOWETL_POSTGRES_PASSWORD | Superuser password for FlowETL's backing database | |

We recommend running FlowETL using the celery scheduler, in which case you will also need to provide additional environment variables and secrets as described in the [main airflow documentation](https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html). 

!!!note
    Any environment variable can be provided as a secret with the same name for the FlowETL container.


!!!note 
    Generating Fernet keys
    
    A convenient way to generate Fernet keys is to use the python [cryptography](https://cryptography.io/) package. After installing, you can generate a new key by running `python -c "from cryptography.fernet import Fernet;print(Fernet.generate_key().decode())"`.


See also the [airflow documentation](https://airflow.apache.org/docs/stable/) for other configuration options which you can provide as environment variables.

The [ETL](management/etl/etl.md) documentation gives detail on how to use FlowETL to load data into FlowDB.

##### Sample stack files
###### FlowDB

You can find a sample FlowDB stack file [here](https://github.com/Flowminder/FlowKit/blob/master/secrets_quickstart/flowdb.yml). To use it, you should first create the secrets, and additionally set the following environment variables:

| Variable name | Purpose | 
| ------------- | ------- |
| FLOWDB_HOST_PORT | Localhost port where FlowDB will be accessible |
| FLOWDB_HOST_USER_ID | uid of the host user for FlowDB |
| FLOWDB_HOST_GROUP_ID | gid of the host user for FlowDB |
| FLOWDB_DATA_DIR | Path on the host to a directory owned by FLOWDB_HOST_USER_ID where FlowDB will store data and write logs |
| FLOWDB_ETL_DIR | Path on the host to a directory readable by FLOWDB_HOST_USER_ID from which data may be loaded, mounted inside the container at /etl |

Once the FlowDB service has started, you will be able to access it using `psql` as with any standard PostgreSQL database.

###### FlowETL

You can find a sample FlowETL stack file [here](https://github.com/Flowminder/FlowKit/blob/master/secrets_quickstart/flowetl.yml) which should be used with the [FlowDB stack file](https://github.com/Flowminder/FlowKit/blob/master/secrets_quickstart/flowdb.yml). To use it, you should first create the required secrets, and additionally set the following environment variables:

| Variable name | Purpose | 
| ------------- | ------- |
| FLOWETL_HOST_PORT | Localhost port on which the FlowETL airflow web interface will be available | 
| FLOWETL_HOST_USER_ID | uid of the host user for FlowETL |
| FLOWETL_HOST_GROUP_ID | gid of the host user for FlowETL |
| FLOWETL_HOST_DAG_DIR | Path on the host to a directory where dag files will be stored |
| FLOWETL_WORKER_COUNT | The number of workers which will be available to run tasks |
| FLOWETL_CELERY_PORT | Port which the Celery user interface will be available on |

Once your stack has come up, you will be able to access FlowETL's web user interface which allows you to monitor the progress of ETL tasks. 

### FlowDB, FlowETL and FlowMachine

For cases where your users require individual level data access, you can support the use of FlowMachine as a library. In this mode, users connect directly to FlowDB via the [FlowMachine Python module](https://pypi.org/project/flowmachine/). Many of the benefits of a complete FlowKit deployment are available in this scenario, including query caching.

You will need to host a redis service, to allow the FlowMachine users to coordinate processing. See the [FlowMachine stack file](https://github.com/Flowminder/FlowKit/blob/master/secrets_quickstart/flowmachine.yml) for an example of deploying redis using docker.

You will need to [create database users](management/users.md) for each user who needs access, and provide them with the password to the redis instance. Users should install FlowMachine individually, using pip (`pip install flowmachine`).

!!!note
    A FlowMachine Docker service is not required for using FlowMachine as a library - users can install the FlowMachine module individually. A redis service is required, and all users should connect to the same redis instance.

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

#### FlowAuth

FlowAuth is designed to be deployed as a single Docker container working in cooperation with a database and, typically, an ssl reverse proxy (e.g. [nginx-proxy](https://github.com/jwilder/nginx-proxy) combined with [letsencrypt-nginx-proxy-companion](https://github.com/JrCs/docker-letsencrypt-nginx-proxy-companion)).

To run FlowAuth, you should set the following secrets:

| Secret name | Secret purpose |
| ----------- | -------------- |
| FLOWAUTH_ADMIN_USER | Admin user name |
| FLOWAUTH_ADMIN_PASSWORD | Admin user password |
| FLOWAUTH_DB_PASSWORD | Password for FlowAuth's backing database |
| FLOWAUTH_FERNET_KEY | Reversible encryption key for storing tokens at rest |
| SECRET_KEY | Secures session and CSRF protection cookies |
| PRIVATE_JWT_SIGNING_KEY | Used to sign the tokens generated by FlowAuth, which ensures that they can be trusted by FlowAPI | 

You may also set the following environment variables:

| Variable name | Purpose | Notes |
| ------------- | ------- | ----- |
| DB_URI | URI for the backing database | Should be of the form `postgresql://flowauth:{}@flowauth_postgres:5432/flowauth`. If not set, a temporary sqlite database will be created. The `{}` will be populated using the value of the `FLOWAUTH_DB_PASSWORD` secret. | 
| RESET_FLOWAUTH_DB | Set to true to reset the database | |
| FLOWAUTH_CACHE_BACKEND | Backend to use for two factor auth last used key cache | Defaults to 'file'. May be set to 'memory' if deploying a single instance on only one CPU, or to 'redis' for larger deployments |

##### Two-factor authentication

FlowAuth supports optional two-factor authentication for user accounts, using the Google Authenticator app or similar. This can be enabled either by an administrator, or by individual users.

To safeguard two-factor codes, FlowAuth prevents users from authenticating more than once with the same code within a short window. When deploying to production, you may wish to deploy a redis backend to support this feature - for example if you are deploying multiple instances of the FlowAuth container which need to be able to record the last used codes for users in a common place.

To configure FlowAuth for use with redis, set the `FLOWAUTH_CACHE_BACKEND` environment variable to `redis`. You will also need to set the following secrets:

| Secret name | Purpose | Default |
| ----------- | ------- | ----- |
| FLOWAUTH_REDIS_HOST | The hostname to connect to redis on. | |
| FLOWAUTH_REDIS_PORT | The port to use to connect to redis | 6379 |
| FLOWAUTH_REDIS_PASSWORD | The password for the redis database | |
| FLOWAUTH_REDIS_DB | The database _number_ to connect to | 0 |
    
By default, FlowAuth will use a dbm file backend to track last used two-factor codes. This file will be created at `/dev/shm/flowauth_last_used_cache` inside the container (i.e. in Docker's shared memory area), and can be mounted to a volume or pointed to an alternative location by setting the  `FLOWAUTH_CACHE_FILE` environment variable.

##### Sample stack files

You can find an example docker stack file for FlowAuth [here](https://github.com/Flowminder/FlowKit/blob/master/flowauth/docker-compose.yml). This will bring up instances of FlowAuth, redis, and postgres. You can combine this with the [letsencrypt](https://github.com/Flowminder/FlowKit/blob/master/flowauth/docker-compose.letsencrypt.yml) stack file to automatically acquire an SSL certificate.

#### FlowMachine and FlowAPI

Once you have FlowAuth, FlowDB, and FlowETL running, you are ready to add FlowMachine and FlowAPI.

##### FlowMachine

The FlowMachine server requires one additional secret: `REDIS_PASSWORD`, the password for an accompanying redis database. This secret should also be provided to redis. FlowMachine also uses the `FLOWMACHINE_FLOWDB_USER` and `FLOWMACHINE_FLOWDB_PASSWORD` secrets defined for FlowDB.

You may also set the following environment variables:

| Variable name | Purpose | Default |
| ------------- | ------- | ----- |
| FLOWMACHINE_PORT | Port FlowAPI should communicate on | 5555 |
| FLOWMACHINE_SERVER_DEBUG_MODE | Set to True to enable debug mode for asyncio  | False |
| FLOWMACHINE_SERVER_DISABLE_DEPENDENCY_CACHING | Set to True to disable automatically pre-caching dependencies of running queries | False |
| FLOWMACHINE_CACHE_PRUNING_FREQUENCY | How often to automatically clean up the cache |  86400 (24 hours) |
| FLOWMACHINE_CACHE_PRUNING_TIMEOUT | Number of seconds to wait before halting a cache prune | 600 |
| FLOWMACHINE_LOG_LEVEL | Verbosity of logging (critical, error, info, or debug) | error |
| FLOWMACHINE_SERVER_THREADPOOL_SIZE | Number of threads the server will use to manage running queries | 5*n_cpus |
| DB_CONNECTION_POOL_SIZE | Number of connections keep open to FlowDB - the server can actively run this many queries at once. You may wish to increase this if the FlowDB instance is running on a powerful server with multiple CPUs | 5 |
| DB_CONNECTION_POOL_OVERFLOW |  Number of connections in addition to `DB_CONNECTION_POOL_SIZE` to open if needed | 1 |

#### FlowAPI

FlowAPI requires additional secrets:

| Secret name | Purpose | Notes |
| ----------- | ------- | ----- |
| cert-flowkit.pem | SSL Certificate used to serve FlowAPI over https | Optional, but _strongly_ recommended. If you are using a self-signed certificate, you will need to make the file available to FlowClient users. |
| key-flowkit.pem | Private key for the SSL Certificate | Optional, but _strongly_ recommended. This part of the certificate does _not_ need to be made available to FlowClient users. |
| PUBLIC_JWT_SIGNING_KEY | Public key to verify api tokens | The public key corresponding to the `PRIVATE_JWT_SIGNING_KEY` used by FlowAuth
| FLOWAPI_IDENTIFIER | Secret used in combination with secret key for decoding JWTs | Should be unique per FlowAPI server; this will also be the _name_ of the server in the FlowAuth user interface |

FlowAPI also makes use of the `FLOWAPI_FLOWDB_USER` and `FLOWAPI_FLOWDB_PASSWORD` secrets provided to FlowDB.

##### Adding the new server to FlowAuth

Once FlowAPI has started, it can be added to FlowAuth so that users can generate tokens for it. You should be able to download the API specification from `https://<flowapi_host>:<flowapi_port>/api/0/spec/openapi.json`. You can then use the spec file to add the server to FlowAuth by navigating to Servers, and clicking the new server button.

After uploading the specification, you can configure the maximum token lifetime settings, and use the dropdown box to enable or disable access to the available FlowAPI scopes. If you have updated either the FlowAPI or FlowMachine servers, you should upload the newly generated specification to ensure that the correct API actions are available when assigning users and generating tokens. 

##### Sample stack files

###### FlowMachine

A sample stack file suitable for use with the FlowDB and FlowETL stacks can be found [here](https://github.com/Flowminder/FlowKit/blob/master/secrets_quickstart/flowmachine.yml). This adds an additional two services: FlowMachine, and a redis instance used to coordinate the running state of queries. If you are supporting additional users with FlowMachine as a library, they should also use this redis instance. This stack file requires one additional environment variable: `REDIS_HOST_PORT`, the localhost port where Redis will be accessible.

###### FlowAPI

The sample stack file for FlowAPI can be found [here](https://github.com/Flowminder/FlowKit/blob/master/secrets_quickstart/flowapi.yml), and requires one additional environment variable: `FLOWAPI_HOST_PORT`, the local port to make the API accessible on. 


##### Secrets Quickstart

A full example deployment script which brings up all components is available [here](https://github.com/Flowminder/FlowKit/blob/master/secrets_quickstart/secrets_quickstart.sh).

This will bring up a single node swarm, create random 16 character passwords for the database users, generate a fresh RSA key pair which links FlowAuth and FlowAPI, generate a certificate valid for the `flowkit.api` domain (and point that to `localhost` using `/etc/hosts`), pull all necessary containers, and bring up FlowAuth and FlowAPI.

For convenience, you can also do `pipenv run secrets_quickstart` from the `secrets_quickstart` directory.

Note that if you wish to deploy a branch other than `master`, you should set the `CONTAINER_TAG` environment variable before running, to ensure that Docker pulls the correct tags.

You can then provide the certificate to `flowclient`, and finally connect via https:

```python
import flowclient
conn = flowclient.Connection(url="https://localhost:9090", token="JWT_STRING", ssl_certificate="<path_to_cert.pem>")
```

(This generates a certificate valid for the `flow.api` domain as well, which you can use by adding a corresponding entry to your `/etc/hosts` file.)


#### AutoFlow production deployment

Analysts with permission to run docker containers may choose to run their own AutoFlow instances. Instructions for doing so can be found in the [AutoFlow documentation](../analyst/autoflow.md#running-autoflow). A sample stack file for deploying AutoFlow along with the rest of the FlowKit stack can be found [here](https://github.com/Flowminder/FlowKit/blob/master/autoflow/docker-stack.yml), which adds an AutoFlow service, and an additionl Postgres database used by AutoFlow to record workflow runs. This makes use of the `cert-flowkit.pem` secret provided to FlowAPI, and also requires two other secrets:

| Secret name | Secret purpose |
| ----------- | -------------- |
| AUTOFLOW_DB_PASSWORD | Password for AutoFlow's database |
| FLOWAPI_TOKEN | API token AutoFlow will use to connect to FlowAPI |

You should also set the following environment variables:

| Variable name | Purpose |
| ------------- | ------- |
| AUTOFLOW_INPUTS_DIR | Path on the host to the directory where input files to AutoFlow are stored |
| AUTOFLOW_OUTPUTS_DIR | Path on the host to a directory where AutoFLow should store output files |

and optionally set the `AUTOFLOW_LOG_LEVEL` environment variable (default 'ERROR').

!!!note
    AutoFlow input files (Jupyter notebooks and `workflows.yml`) should be in the inputs directory before starting the AutoFlow container. Files added later will not be picked up by AutoFlow.

#### Demonstrating successful deployment

Once FlowKit installation is complete, you can verify that the system has been successfully set up by visiting `http<s>://<flowapi_url>:<flowapi_port>/api/0/spec/redoc`. Once all the services have come up, you will be able to view the interactive API specification.
We also recommend running the provided [worked examples](../analyst/worked_examples/index.md) against the deployed FlowKit to check that everything is working correctly.

## Additional support

If you require more assistance to get up and running, please reach out to us by [email](mailto:flowkit@flowminder.org) and we will try to assist.
