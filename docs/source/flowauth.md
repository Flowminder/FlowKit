# FlowAuth

FlowAuth provides centralised authentication management for FlowKit systems.

FlowAuth allows users to generate access tokens for use with [FlowClient](../flowclient).

## Quickstart

To run a demonstration version of FlowAuth use:

```bash
docker run -p 8000:80 -e DEMO_MODE=1 flowminder/flowauth
```

This will start FlowAuth, and pre-populate a disposable sqlite database with some dummy data. You can then visit http://localhost:8000 and log in with either `TEST_ADMIN:DUMMY_PASSWORD` or `TEST_USER:DUMMY_PASSWORD`.


## Deployment

FlowAuth is designed to be deployed as a single Docker container working in cooperation with a database and, typically, an ssl reverse proxy (e.g. [nginx-proxy](https://github.com/jwilder/nginx-proxy) combined with [letsencrypt-nginx-proxy-companion](https://github.com/JrCs/docker-letsencrypt-nginx-proxy-companion)).

FlowAuth supports any database supported by [SQLAlchemy](https://sqlache.me), and to connect you will only need to supply a correct URI for the database either using the `DB_URI` environment variable, or by setting the `DB_URI` secret. If `DB_URI` is not set, a temporary sqlite database will be created.

On first use, you will need to create the necessary tables and an administrator account. 

To initialise the tables, you can either set the `INIT_DB` environment variable to `true`, or run `flask init-db` from inside the container (`docker exec <container-id> flask init-db`).

To create an initial administrator, use the `ADMIN_USER` and `ADMIN_PASSWORD` environment variables or set them as secrets. Alternatively, you may run `flask add-admin <username> <password>` from inside the container. You can combine these environment variables with the `INIT_DB` environment variable.

You _must_ also provide two additional environment variables or secrets: `FERNET_KEY`, and `SECRET_KEY`. `FERNET_KEY` is used to encrypt server secret keys, and tokens while 'at rest' in the database, and decrypt them for use. `SECRET_KEY` is used to secure session, and CSRF protection cookies.

By default, `SECRET_KEY` will be set to `secret`. You should supply this to ensure a secure system.

While `SECRET_KEY` can be any arbitrary string, `FERNET_KEY` should be a valid Fernet key. A convenience command is provided to generate one - `flask get-fernet`.  

## Generating tokens

The following steps are required to add a user and allow them to generate access tokens to communicate with a FlowKit server:

1. Log into FlowAuth as an administrator.

2. Under "API Routes", add any applicable API routes (e.g. `daily_location`).

3. Under "Aggregation Units", add any applicable aggregation units (e.g. `admin3`).

3. Under "Servers", add a new server and give it a name and secret key. Note that the Secret Key must match the `JWT_SECRET_KEY` set in the flowapi docker container on this server.

4. Enable any permissions for this server under "API Permissions", and aggregation units under "Aggregation Units".

5. Under "Users", add a new user, and set the username and password.

6. Either:

    - Add a server to the user, and enable/disable API permissions and aggregation units,

7. Or:

    - Under "Groups", add a new group,

    - Add a server to the group, and enable/disable API permissions and aggregation units,

    - Add the user to the group.


The user can then log in and generate a token:

1. Log into FlowAuth using the username and password created by the administrator.

2. Optionally, click on the person icon (top right) and reset password.

3. Select the server under "My Servers".

4. Click the '+' icon to add a token, and give it a name (and optionally change the expiry and permissions).

5. Click "TOKEN" to display the token string.
