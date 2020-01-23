## FlowAuth

### Two-factor authentication

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

### Granting user permissions 

FlowAuth is the tool which analysts will use to generate tokens which will allow them to communicate with a FlowKit server through FlowAPI. The following steps using the FlowAuth administration tool are required to add a user and allow them to generate access tokens:

1. Log into FlowAuth as an administrator.

2. Under "API Routes", add any applicable API routes (e.g. `daily_location`).

3. Under "Aggregation Units", add any applicable aggregation units (e.g. `admin3`).

3. Under "Servers", add a new server and give it a name. Note that the name must match the `FLOWAPI_IDENTIFIER` variable set in the FlowAPI docker container on this server.

4. Enable any permissions for this server under "API Permissions", and aggregation units under "Aggregation Units".

5. Under "Users", add a new user, and set the username and password.

6. Either:
    - Add a server to the user, and enable/disable API permissions and aggregation units,
    <p>
7. Or:
    <p>
    - Under "Groups", add a new group,

    - Add a server to the group, and enable/disable API permissions and aggregation units,

    - Add the user to the group.

The user can then log into FlowAuth and generate a token (see the [analyst section](../analyst/index.md#flowauth) for instructions).