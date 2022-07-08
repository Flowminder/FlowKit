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

### Exposing a new query kind

The FlowMachine server is responsible for defining the queries that users can run via FlowAPI. Translation from parameters provided in calls to the `run` API endpoint to the underlying FlowMachine query objects is handled by [marshmallow](https://marshmallow.readthedocs.io) schemas defined in [flowmachine.core.server.query_schemas](../flowmachine/flowmachine/core/server/query_schemas/).

In this section we assume that a FlowMachine query class `MyQuery` (derived from [`flowmachine.core.query.Query`](../flowmachine/flowmachine/core/query/#class-query)) is already defined, and describe the steps required to expose this query class, so that queries of this kind can be run via the API.

The information below refers to the following categories of query:

- **aggregate**: A query whose results are aggregated over groups of subscribers, so that no individual-level information is revealed. Example: [histogram_aggregate](../flowmachine/flowmachine/core/server/query_schemas/histogram_aggregate/)
- **spatial aggregate**: An aggregate query that returns a result per location (e.g. a count of subscribers per administrative region). Example: [spatial_aggregate](../flowmachine/flowmachine/core/server/query_schemas/spatial_aggregate/)
- **individual-level query**: A query whose result consists of a value per subscriber. Example: [event_count](../flowmachine/flowmachine/core/server/query_schemas/event_count/)
- **reference location**: An individual-level query that assigns a single location to each subscriber. Example: [daily_location](../flowmachine/flowmachine/core/server/query_schemas/daily_location/)

#### 1. Define an "exposed query" class

In a new file 'flowmachine/core/server/query_schemas/my_query.py', define a new class `MyQueryExposed`. This class is responsible for constructing the appropriate `MyQuery` object from parameter values supplied in an API call.

There are two options here: if users should be able to select a random sample of the rows from this query result, the exposed query class should inherit from [`BaseExposedQueryWithSampling`](../flowmachine/flowmachine/core/server/query_schemas/base_query_with_sampling/#class-baseexposedquerywithsampling) (this is usually appropriate for _individual-level_ queries). If it does not make sense to allow random sampling of the query result (as is usually the case for _aggregate_ queries), the exposed query class should inherit from [`BaseExposedQuery`](../flowmachine/flowmachine/core/server/query_schemas/base_exposed_query/#class-baseexposedquery).

=== "Without sampling"

    ```python
    from flowmachine.core.server.query_schemas.base_exposed_query import BaseExposedQuery

    class MyQueryExposed(BaseExposedQuery):
        query_kind = "daily_location" # (1)

        def __init__( # (2)
            self,
            *,
            start_date,
            end_date,
            sub_query,
            other_param,
        ):
            self.start_date = start_date # (3)
            self.end_date = end_date
            self.sub_query = sub_query
            self.other_param = other_param
        
        @property
        def aggregation_unit(self): # (4)
            return self.sub_query.aggregation_unit
        
        @property
        def _flowmachine_query_obj(self): # (5)
            return MyQuery(
                start=self.start_date, # (6)
                stop=self.end_date,
                sub_query=self.sub_query._flowmachine_query_obj, # (7)
                other_param=self.other_param,
                non_exposed_param="default_value", # (8)
            )
    ```

    1.  `query_kind` class attribute is required, and must be different from the `query_kind` of all other exposed query classes.
    2.  The `__init__` method should take as arguments all parameters of `MyQuery` that will be exposed via the API.
    3.  All input parameters must be set as attributes on `self` so that the object can be serialised correctly.
    4.  If `MyQuery` is a _spatial aggregate_ or a _reference location_, but does not have an explicit `aggregation_unit` parameter (e.g. because the aggregation unit is determined by a nested sub-query), you must define an `aggregation_unit` property or attribute so that other queries (and the `get_aggregation_unit` server action) can identify the aggregation unit associated with this query.
    5.  Define a `_flowmachine_query_obj` property that returns the underlying `MyQuery` FlowMachine query object.
    6.  The exposed parameters do not need to have names that match the corresponding parameters of the underlying `MyQuery` object.
    7.  If a parameter is a nested sub-query, you will need to access its `_flowmachine_query_obj` property so that the `MyQuery` constructor receives a Flowmachine query object and not the _exposed_ query object.
    8.  It is not necessary for all parameters of the underlying `MyQuery` object to be exposed as parameters of `MyQueryExposed`.

=== "With sampling"

    ```python
    from flowmachine.core.server.query_schemas.base_query_with_sampling import BaseExposedQueryWithSampling

    class MyQueryExposed(BaseExposedQueryWithSampling):
        query_kind = "daily_location" # (1)

        def __init__( # (2)
            self,
            *,
            start_date,
            end_date,
            sub_query,
            other_param,
            sampling=None, # (9)
        ):
            self.start_date = start_date # (3)
            self.end_date = end_date
            self.sub_query = sub_query
            self.other_param = other_param
            self.sampling = sampling
        
        @property
        def aggregation_unit(self): # (4)
            return self.sub_query.aggregation_unit
        
        @property
        def _unsampled_query_obj(self): # (5)
            return MyQuery(
                start=self.start_date, # (6)
                stop=self.end_date,
                sub_query=self.sub_query._flowmachine_query_obj, # (7)
                other_param=self.other_param,
                non_exposed_param="default_value", # (8)
            )
    ```

    1.  `query_kind` class attribute is required, and must be different from the `query_kind` of all other exposed query classes.
    2.  The `__init__` method should take as arguments all parameters of `MyQuery` that will be exposed via the API.
    3.  All input parameters must be set as attributes on `self` so that the object can be serialised correctly.
    4.  If `MyQuery` is a _spatial aggregate_ or a _reference location_, but does not have an explicit `aggregation_unit` parameter (e.g. because the aggregation unit is determined by a nested sub-query), you must define an `aggregation_unit` property or attribute so that other queries (and the `get_aggregation_unit` server action) can identify the aggregation unit associated with this query.
    5.  Define a `_unsampled_query_obj` property that returns the underlying `MyQuery` FlowMachine query object. **Note:** When inheriting from `BaseExposedQueryWithSampling`, this property should be named `_unsampled_query_obj` - the `_flowmachine_query_obj` property will return this query wrapped in an appropriate "random sample" query.
    6.  The exposed parameters do not need to have names that match the corresponding parameters of the underlying `MyQuery` object.
    7.  If a parameter is a nested sub-query, you will need to access its `_flowmachine_query_obj` property so that the `MyQuery` constructor receives a Flowmachine query object and not the _exposed_ query object.
    8.  It is not necessary for all parameters of the underlying `MyQuery` object to be exposed as parameters of `MyQueryExposed`.
    9.  When inheriting from `BaseExposedQueryWithSampling`, it is important to also accept the `sampling` argument here.


#### 2. Define a "query schema" class

In the same file as `MyQueryExposed`, define a new class `MyQuerySchema`. This is a [marshmallow](https://marshmallow.readthedocs.io) schema, responsible for validation and deserialisation of parameter values supplied in an API call.

As before, there are two options, depending on whether or not random sampling should be enabled for this query kind. If `MyQueryExposed` inherits from `BaseExposedQueryWithSampling` then `MyQuerySchema` should inherit from [`BaseQueryWithSamplingSchema`](../flowmachine/flowmachine/core/server/query_schemas/base_query_with_sampling/#class-basequerywithsamplingschema). Otherwise, `MyQuerySchema` should inherit from [`BaseSchema`](../flowmachine/flowmachine/core/server/query_schemas/base_schema/#class-baseschema).

=== "Without sampling"

    ```python
    from marshmallow import fields, validate
    from flowmachine.core.server.query_schemas.field_mixins import StartAndEndField
    from flowmachine.core.server.query_schemas.base_schema import BaseSchema
    from flowmachine.core.server.query_schemas.aggregation_unit import AggregationUnitKind

    class MyQuerySchema(
        StartAndEndField, # (1)
        BaseSchema,
    ):
        __model__ = MyQueryExposed # (2)

        query_kind = fields.String( # (3)
            validate=validate.OneOf([__model__.query_kind]),
            required=True,
        )
        sub_query = fields.Nested(SomeOtherQuerySchema, required=True) # (4)
        other_param = fields.Integer(
            validate=validate.Range(0, 10), # (5)
            required=False,
            load_default=0, # (6)
        )
        # Only relevant for spatial aggregates:
        aggregation_unit = AggregationUnitKind(dump_only=True) # (7)
    ```

    1.  The `StartAndEndField` mixin adds `start_date` and `end_date` fields. There are other mixins available for adding commonly-used fields, e.g. [`HoursField`](../flowmachine/flowmachine/core/server/query_schemas/field_mixins/#class-hoursfield) and [`AggregationUnitMixin`](../flowmachine/flowmachine/core/server/query_schemas/aggregation_unit/#class-aggregationunitmixin).
    2.  Set `MyQueryExposed` as the `__model__` class attribute so that `MyQuerySchema` will deserialise parameters to an instance of `MyQueryExposed`.
    3.  `query_kind` field must be defined here. This field will not be passed on to `MyQueryExposed.__init__()`.
    4.  Sub-queries can be accepted as parameters by specifying the appropriate query schema in a marshmallow `Nested` field.
    5.  The fields specified here should provide all necessary validation of parameter values.
    6.  If you wish to set a default parameter value to be used if no value is supplied by the user, it is better to specify this here than in `MyQueryExposed.__init__()` so that the default value will be stated in the API spec.
    7.  If `MyQuery` is a _spatial aggregate_ but does not have an explicit `aggregation_unit` parameter (e.g. because the aggregation unit is determined by a nested sub-query), add a _dump-only_ 'aggregation_unit' field. This enables FlowAPI to identify this query kind as a spatial aggregate, without exposing a redundant 'aggregation_unit' input parameter.

=== "With sampling"

    ```python
    from marshmallow import fields, validate
    from flowmachine.core.server.query_schemas.field_mixins import StartAndEndField
    from flowmachine.core.server.query_schemas.base_query_with_sampling import BaseQueryWithSamplingSchema
    from flowmachine.core.server.query_schemas.aggregation_unit import AggregationUnitKind

    class MyQuerySchema(
        StartAndEndField, # (1)
        BaseQueryWithSamplingSchema, # (8)
    ):
        __model__ = MyQueryExposed # (2)

        query_kind = fields.String( # (3)
            validate=validate.OneOf([__model__.query_kind]),
            required=True,
        )
        sub_query = fields.Nested(SomeOtherQuerySchema, required=True) # (4)
        other_param = fields.Integer(
            validate=validate.Range(0, 10), # (5)
            required=False,
            load_default=0, # (6)
        )
        # Only relevant for spatial aggregates:
        aggregation_unit = AggregationUnitKind(dump_only=True) # (7)
    ```

    1.  The `StartAndEndField` mixin adds `start_date` and `end_date` fields. There are other mixins available for adding commonly-used fields, e.g. [`HoursField`](../flowmachine/flowmachine/core/server/query_schemas/field_mixins/#class-hoursfield) and [`AggregationUnitMixin`](../flowmachine/flowmachine/core/server/query_schemas/aggregation_unit/#class-aggregationunitmixin).
    2.  Set `MyQueryExposed` as the `__model__` class attribute so that `MyQuerySchema` will deserialise parameters to an instance of `MyQueryExposed`.
    3.  `query_kind` field must be defined here. This field will not be passed on to `MyQueryExposed.__init__()`.
    4.  Sub-queries can be accepted as parameters by specifying the appropriate query schema in a marshmallow `Nested` field.
    5.  The fields specified here should provide all necessary validation of parameter values.
    6.  If you wish to set a default parameter value to be used if no value is supplied by the user, it is better to specify this here than in `MyQueryExposed.__init__()` so that the default value will be stated in the API spec.
    7.  If `MyQuery` is a _spatial aggregate_ but does not have an explicit `aggregation_unit` parameter (e.g. because the aggregation unit is determined by a nested sub-query), add a _dump-only_ 'aggregation_unit' field. This enables FlowAPI to identify this query kind as a spatial aggregate, without exposing a redundant 'aggregation_unit' input parameter.
    8.  `BaseQueryWithSamplingSchema` adds a `sampling` field.

#### 3. Expose the query

If `MyQuery` is an _aggregate_, it can be exposed as a top-level query (meaning that API users will be able to directly run and get the results of `MyQuery` queries). In this case, add `MyQuerySchema` to [`FlowmachineQuerySchema.query_schemas`](../flowmachine/flowmachine/core/server/query_schemas/flowmachine_query/#class-flowmachinequeryschema).

!!! warning

    If the query will be exposed as a top-level query, it is essential that the underlying FlowMachine query defined in the exposed query's `_flowmachine_query_object` property is _redacted_ - i.e. all rows in the query result corresponding to 15 or fewer individuals are removed from the output. This protects individuals' privacy through k-anonymity.

If `MyQuery` is an _individual-level_ query, it should **not** be exposed directly as a top-level query. In this case, `MyQuerySchema` should be added as a nested sub-query parameter of the appropriate other query schemas. For example, if `MyQuery` is a _reference location_, add `MyQuerySchema` to [`ReferenceLocationSchema.query_schemas`](../flowmachine/flowmachine/core/server/query_schemas/reference_location/#class-referencelocationschema) so that it will be accepted as a parameter to query kinds such as `spatial_aggregate` and `flows`.


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
