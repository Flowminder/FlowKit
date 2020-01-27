Title: ETL

## Working with FlowETL

FlowETL manages the loading of CDR data into FlowDB. It is built on [Apache Airflow](https://airflow.apache.org), and a basic understanding of how to use Airflow will be very helpful in making the best use of FlowETL. We recommend you familiarise yourself with the Airflow [tutorial](https://airflow.apache.org/docs/stable/tutorial.html), and [key concepts](https://airflow.apache.org/docs/stable/concepts.html) before continuing. You should also have some familiarity with SQL.

### Macros

To help you write the SQL you'll need to create your data pipeline, FlowETL supplies several macros, in additional to those [provided by Airflow](https://airflow.apache.org/docs/stable/macros.html). These macros are filled in when a task is run.

| Macro | Purpose | Example |
| ----- | ------- | ------- |
| `{{ params.cdr_type }}` | The category of CDR data being loaded | `"calls"` |
| `{{ table_name }}` | The base name of the table from the date and cdr type | `"mds_20200101"` |
| `{{ etl_schema }}` | Name of the schema used for etl tables | `"etl"` |
| `{{ final_schema }}` |  Schema under which the final table will be created | `"events"` |
| `{{ parent_table }}` |  Supertable that the final table will inherit from | `"calls"` |
| `{{ extract_table_name }}` | Name of the table created to extract to | `"extract_sms_20200101"` |
| `{{ staging_table_name }}` | Name of the table or view created to extract _from_ | `"staging_sms_20200101"` |
| `{{ final_table }}` | Schema qualified name of the final table | `"events.mds_20200101"` |
| `{{ extract_table }}` | Schema qualified name of the table to be extracted to | `"etl.extract_mds_20200101"` |
| `{{ staging_table }}` | Schema qualified name of the table to be extracted _from_ | `"etl.staging_mds_20200101"` |

These macros are available when using the FlowETL [operators](../../../../flowetl/flowetl/operators) and [sensors](../../../../flowetl/flowetl/sensors).

### Creating ETL pipelines

As with any Airflow-based system, you will need to create dag files to define your ETL pipelines. FlowETL provides a convenient helper function, [`create_dag`](../../../../flowetl/flowetl/util/#create_dag), which automates this for most common usage scenarios. To create a pipeline, you will need to create a new file in the directory which you have bind mounted to the FlowETL container's DAG directory. This file will specify one data pipeline - we recommend creating a separate data pipeline for each CDR variant you expect to encounter.

You can find some example pipelines in the FlowETL section of our [GitHub repository](https://github.com/Flowminder/FlowKit/tree/master/flowetl/mounts/dags).

Because data comes in many forms, you must specify in SQL a transformation from your source data to the correct FlowDB schema for the CDR data type. You will need to write this transformation in the form of a `SELECT` statement, using the `{{ staging_table }}` macro as the source table.

!!!warning

    When specifing a transform, you may not have (or need) all the fields specified in the schema. Any fields which are not included _still need to be specified in the transform_.
    Field which are not being extracted from your source data should be specified as `NULL::<field_type>`, and fields _must_ be specified in your select statement in the other they are given in the tables below. 
    
    For example, a valid SQL extract statement for calls data with _only_ the mandatory fields available:
    
    ```sql
    SELECT uuid_generate_v4()::TEXT as id, TRUE as outgoing, event_time::TIMESTAMPTZ as datetime, NULL::NUMERIC as duration,
    NULL::TEXT as network, msisdn::TEXT as msisdn, NULL::TEXT as msisdn_counterpart, NULL::TEXT as location_id, NULL::TEXT as imsi,
    NULL::TEXT as imei, NULL::NUMERIC(8) as tac, NULL::NUMERIC as operator_code, NULL::NUMERIC as country_code

    FROM {{ staging_table }}
    ``` 

#### Calls data

FlowDB uses a two-line format for calls data, similar to double entry bookkeeping. One row records information about the msisdn, location etc. of the _calling_ party, and a second records the equivalent information for the receiver.
If your data is supplied in _one_-line format, you will need to transform it to two-lines as part of the extract step.

The result of your extract SQL must conform to this schema:

| Field | Type | Optional | Notes |
| ----- | ---- | -------- | ----- |
| id | TEXT | ✓ | ID which matches this call to the counterparty record if available |
| outgoing | BOOLEAN | ✓ | True if this is the initiating record for the call |
| datetime | TIMESTAMPTZ | ✗ | The time the call began (we recommend storing this in UTC) |
| duration | NUMERIC | ✓ | The length of time in fractional minutes that call lasted |
| network | TEXT | ✓ | The network this party was on |
| msisdn | TEXT | ✗ | The (deidentified) MSISDN of this party's phone |
| msisdn_counterpart | TEXT | ✓ | The (deidentified) MSISDN of the other party's phone |
| location_id | TEXT | ✓ | The ID of the location, which should refer to a cell in `infrastructure.cells` |
| imsi | TEXT | ✓ | The (deidentified) IMSI of this party's phone |
| imei | TEXT | ✓ | The (deidentified) IMEI of this party's phone |
| tac | NUMERIC(8) | ✓ | The TAC code of this party's handset |
| operator_code | NUMERIC | ✓ | The numeric code of this party's operator |
| country_code | NUMERIC | ✓ | The numeric code of this party's country |

#### SMS data

As with calls, FlowDB uses a two-line format for sms data. One row records information about the msisdn, location etc. of the _sender_ , and a second records the equivalent information for any receivers.

The result of your extract SQL must conform to this schema:

| Field | Type | Optional | Notes |
| ----- | ---- | -------- | ----- |
| id | TEXT | ✓ | ID which matches this sms to the counterparty record if available |
| outgoing | BOOLEAN | ✓ | True if this is the initiating record for the sms |
| datetime | TIMESTAMPTZ | ✗ | The time the sms was sent (we recommend storing this in UTC) |
| network | TEXT | ✓ | The network this party was on |
| msisdn | TEXT | ✗ | The (deidentified) MSISDN of this party's phone |
| msisdn_counterpart | TEXT | ✓ | The (deidentified) MSISDN of the other party's phone |
| location_id | TEXT | ✓ | The ID of the cell which handled this sms, which should refer to a cell in `infrastructure.cells` |
| imsi | TEXT | ✓ | The (deidentified) IMSI of this party's phone |
| imei | TEXT | ✓ | The (deidentified) IMEI of this party's phone |
| tac | NUMERIC(8) | ✓ | The TAC code of this party's handset |
| operator_code | NUMERIC | ✓ | The numeric code of this party's operator |
| country_code | NUMERIC | ✓ | The numeric code of this party's country |

#### Mobile data session (MDS) data

Because there is no counterparty for MDS data, FlowDB uses a one-line format for this data type, with the following schema:

| Field | Type | Optional | Notes |
| ----- | ---- | -------- | ----- |
| id | TEXT | ✓ | ID which matches this call to the counterparty record if available |
| datetime | TIMESTAMPTZ | ✗ | The time the data session began (we recommend storing this in UTC) |
| duration | NUMERIC | ✓ | The length of time in fractional minutes that data session lasted |
| volume_total | NUMERIC | ✓ | Data volume transferred in MB |
| volume_upload | NUMERIC | ✓ | Data volume sent in MB |
| volume_download | NUMERIC | ✓ | Data volume downloaded in MB |
| network | TEXT | ✓ | The network this party was on |
| msisdn | TEXT | ✗ | The (deidentified) MSISDN of this party's phone |
| location_id | TEXT | ✓ | The ID of the cell which connected this session, which should refer to a cell in `infrastructure.cells` |
| imsi | TEXT | ✓ | The (deidentified) IMSI of the phone |
| imei | TEXT | ✓ | The (deidentified) IMEI of the phone |
| tac | NUMERIC(8) | ✓ | The TAC code of the handset |
| operator_code | NUMERIC | ✓ | The numeric code of the operator |
| country_code | NUMERIC | ✓ | The numeric code of the country |

#### Topup data

As with MDS data, FlowDB uses a one-line format for topups, with the following schema:

| Field | Type | Optional | Notes |
| ----- | ---- | -------- | ----- |
| id | TEXT | ✓ | ID which matches this call to the counterparty record if available |
| datetime | TIMESTAMPTZ | ✗ | The time the data session began (we recommend storing this in UTC) |
| type | TEXT | ✓ | Kind of topup | 
| recharge_amount | NUMERIC | ✓ | Cost of topup | 
| airtime_fee | NUMERIC | ✓ |  | 
| tax_and_fee | NUMERIC | ✓ |   | 
| pre_event_balance | NUMERIC | ✓ |  Balance before topup applied | 
| post_event_balance | NUMERIC | ✓ | Balance after topup applied | 
| msisdn | TEXT | ✗ | The (deidentified) MSISDN of this party's phone |
| location_id | TEXT | ✓ | The ID of the cell which connected this session, which should refer to a cell in `infrastructure.cells` |
| imsi | TEXT | ✓ | The (deidentified) IMSI of the phone |
| imei | TEXT | ✓ | The (deidentified) IMEI of the phone |
| tac | NUMERIC(8) | ✓ | The TAC code of the handset |
| operator_code | NUMERIC | ✓ | The numeric code of the operator |
| country_code | NUMERIC | ✓ | The numeric code of the country |

FlowETL supports a variety of data sources, which are covered in more detail below.

## Connecting to different CDR data sources

### Remote databases

Extracting data from a remote database has significant benefits, because the source database provides guarantees about the integrity of the data in terms of data types, nullable values and so on. FlowDB uses PostgreSQL's foreign data wrapper mechanism to connect to external databases. In general, to use a remote database table as a source you should create a persistent foreign table that will contain the data (instructions of how to do this for the databases supported by FlowETL are given below), then you will need to provide an SQL snippet to use for extraction. For example, to extract three fields:

```sql
SELECT event_time, msisdn, cell_id FROM {{ source_table }}
WHERE event_time >= '{{ ds_nodash }}' AND event_time < '{{ tomorrow_ds_nodash }}';
```

Note the use of the `{{ source_table }}`, which you should provide as an argument to either `create_dag`, or `CreateStagingViewOperator`, and the datetime constraint using the `{{ ds_nodash }}` and  `{{ tomorrow_ds_nodash }}` [Airflow macros](https://airflow.apache.org/docs/stable/macros.html). Remote database extraction in FlowETL works by selecting a time delimited segment of your source table. If your source table is complex, or you will need to use multiple tables to contruct a suitable query, we recommend creating a view in your source database and connecting to the view.

You will also need to be to to connect to the remote database from inside FlowDB's docker container. If the remote database is also running as a container, you can achieve this by creating an overlay network and attaching both containers to it. 

#### PostgreSQL database

The [`postgres_fdw` extension](https://www.postgresql.org/docs/current/postgres-fdw.html) allows FlowDB to communicate with other databases built on PostgreSQL. This makes data extraction from remote PostgreSQL-compatible databases (e.g. [TimescaleDB](https://www.timescale.com)) simple. You will need to create a foreign server, map a user for it, and then specify the table and schema on the remote database you would like to make available within FlowDB:

```sql
CREATE SERVER IF NOT EXISTS ingestion_db_server
    FOREIGN DATA WRAPPER postgres_fdw
    OPTIONS (
        host '<foreign_host>',
        port '<foreign_port>>',
        dbname '<foreign_db_name>'
    );

CREATE USER MAPPING IF NOT EXISTS FOR flowdb
    SERVER foreign_db
    OPTIONS (
        user '<postgres_db_user>',
        password '<postgres_db_password>'
    );

IMPORT FOREIGN SCHEMA <source_table_schema> LIMIT TO (<source_table>)
    FROM SERVER foreign_db INTO etl;
```

You can then use the remote table as a data source in FlowETL, by providing `"<source_table_schema>.<source_table>"` as the source table.

#### Oracle database

FlowDB supports the [oracle_fdw](https://github.com/laurenz/oracle_fdw) connector, but to conform with Oracle's licensing we do not distribute a container which includes it. You will need to supply Oracle client binaries, and build the container by downloading the [Dockerfile and script](https://github.com/Flowminder/FlowKit/tree/master/flowdb/oracle_fdw) and running:

```bash
docker build --build-arg ORACLE_BINARY_SOURCE=<oracle_binary_url> \
              --build-arg CODE_VERSION=latest -t flowdb_with_oracle
```

Once you have built the image, you can use it in place of standard FlowDB.

To connect to Oracle, you will need to access FlowDB as the `flowdb` user and run:

```sql
CREATE SERVER oradb FOREIGN DATA WRAPPER oracle_fdw
          OPTIONS (dbserver '//<oracle_server>:<oracle_port>/<oracle_db>');
CREATE USER MAPPING FOR flowdb SERVER oradb
          OPTIONS (user '<oracle_user>', password '<oracle_password>');
CREATE FOREIGN TABLE oracle_source_table (
          <fields>
       ) SERVER oradb OPTIONS (schema '<ORAUSER>', table '<ORATAB>');
```

Further instructions on use of the wrapper are available from the projects [Github repo](https://github.com/laurenz/oracle_fdw).

#### MSSQL database

### CSV Files

FlowETL can also be used to load data from files - useful if you're receiving data as a daily file dump. To load from a file, you'll need to ensure that the files have a predictable date based name, for example `calls_data_2019_05_01.csv.gz`, which you can capture using a ([templated](https://airflow.apache.org/docs/stable/concepts.html#id1)) string. The filename pattern should include the absolute path to the files from _inside_ your FlowDB container, for example a complete pattern to capture files with names like `calls_data_2019-05-01.csv.gz` in the `/etl/calls` data root might be `/etl/{{ params.cdr_type }}/{{ params.cdr_type }}_data_{{ ds }}`. This uses a combination of Airflow's [built-in macros](https://airflow.apache.org/docs/stable/macros.html#default-variables), and the `{{ params.cdr_type }}` macro supplied by FlowETL.

You will also need to specify the names and [types](https://www.postgresql.org/docs/current/datatype.html) of the fields your CSV will contain as a dict. These will be used to create a [foreign data wrapper](https://www.postgresql.org/docs/current/file-fdw.html) which allows FlowDB to treat your data file like a table, and helps ensure data integrity. You may optionally specify a program to be run to read your data file, for example `zcat` if your source data is compressed. You can either pass these arguments to the [`create_dag`](../../../../flowetl/flowetl/util/#create_dag) function, or use [`CreateForeignStagingTableOperator`](../../../../flowetl/flowetl/operators/create_foreign_staging_table_operator) directly if composing your DAG manually. If you are composing the DAG manually, you will need to use the `ExtractFromForeignTableOperator` to ensure that FlowETL correctly handles cleanup of intermediary ETL steps.

## Loading Infrastructure data

At this time, FlowETL does not provide helper functions loading infrastructure data. You can however still create a custom DAG for regular infrastructure data import using either the `CreateForeignStagingTableOperator` and `ExtractFromForeignTableOperator` operators, the `CreateStagingViewOperator` and `ExtractFromViewOperator`, or by using Airflow's existing [collection of operators](https://airflow.apache.org/docs/stable/_api/index.html#operators-packages).

Cell data should be loaded to the `infrastructure.cells` table, and contain fields as follows:

| Field | Type | Notes |
| ----- | ---- | ----- |
| id  | TEXT | ID of the cell as referenced in the CDR | 
| version  | INTEGER | Add a new row and increment if details of the cell change | 
| site_id  | TEXT | ID of the cell tower this cell is on | 
| name  | TEXT | Any name attached to this cell | 
| type  | TEXT | Type of the cell | 
| msc  | TEXT | Mobile switching centre | 
| bsc_rnc  | TEXT | | 
| antenna_type  | TEXT | | 
| status  | TEXT | | 
| lac  | TEXT | | 
| height  | NUMERIC | | 
| azimuth  | NUMERIC | | 
| transmitter  | TEXT | | 
| max_range  | NUMERIC | (m) | 
| min_range  | NUMERIC |  (m) | 
| electrical_tilt  | NUMERIC | | 
| mechanical_downtilt  | NUMERIC | | 
| date_of_first_service  | DATE | Date this cell became operational | 
| date_of_last_service  | DATE | Date this cell ceased operation | 
| geom_point | POINT | ESPG 4326 point location of the cell |
| geom_polygon | MULTIPOLYGON | ESPG 4326 coverage polygon of the cell | 

This should generally be used in concert with the `infrastructure.sites` table:

| Field | Type | Notes |
| ----- | ---- | ----- |
| id  | TEXT | ID of the site as referenced in the cells table | 
| version  | INTEGER | Add a new row and increment if details of the site change | 
| name  | TEXT | Any name attached to this cell | 
| type  | TEXT | Type of the cell | 
| status  | TEXT | | 
| structure_type  | TEXT | | 
| is_cow  | BOOLEAN | True indicates that this is a mobile tower |
| date_of_first_service  | DATE | Date this site became operational | 
| date_of_last_service  | DATE | Date this site ceased operation | 
| geom_point | POINT | ESPG 4326 point location of the site |
| geom_polygon | MULTIPOLYGON | ESPG 4326 coverage polygon of the site | 

Where site information is not available, we advise that you populate the sites table as a function of the cells table.

## Loading GIS data

Good spatial data is critical to successful analysis using FlowKit, and you will want to obtain, at a minimum admin 0-3 boundaries for your country of interest. At this time, we recommend using the [OGR foreign data wrapper](https://github.com/pramsey/pgsql-ogr-fdw) to load shapefiles.

Administrative boundaries should be loaded under the `geography` schema, and named as `admin<level>`. They should at a minimum contain `adminXname`, `adminXpcod` and `geom` fields. While it is possible to directly mount boundary data into the database using the OGR foreign data wrapper, we would recommend creating in-database tables and using the wrapper only for extraction, for example:


```sql
CREATE SERVER admin_boundaries_source
		FOREIGN DATA WRAPPER ogr_fdw
		OPTIONS (
			datasource '<path_inside_container_to_admin_3_file',
			format 'ESRI Shapefile' );

CREATE FOREIGN TABLE tmp_admin3 (
    gid integer,
    geom geometry(Point, 4326),
    admin3name varchar,
    admin3pcod integer
    )
    SERVER admin_3_source
    OPTIONS (layer 'admin3');

CREATE TABLE geography.admin3 AS (
    SELECT gid, geom, admin3name, admin3pcod FROM
    tmp_admin3);
```

You should also create an index on the `geom` column:

```sql
CREATE INDEX admin3_geom_gist ON geography.admin3 USING gist (geom);
```

## Data QA checks

FlowETL includes a small number of built in QA checks. These checks are not designed to pass or fail newly arriving data, but to provide you and your analysts with important caveats and metadata about the data you are working with. QA checks will run automatically if you are using the [`create_dag`](../../../../flowetl/flowetl/util/#create_dag) function, and their results will be available inside FlowDB in the `etl.post_etl_queries` table to both superusers, and the `flowmachine` role. If you are manually composing a DAG, you can use the [`get_qa_checks`](../../../../flowetl/flowetl/util/#get_qa_checks) function to return a list of QA check tasks, which can be scheduled in relation to the other tasks in the dag.

## Customising ETL pipelines

### Adding new QA checks

Adding additional QA checks is as simple as writing an SQL select statement. To add your own custom checks, create a `qa_checks` subfolder in the directory where you are keeping your dags. You can then create your check as a `<check name.sql>` file, and it will automatically get picked up for the next run of the dag.

SQL files under the `qa_checks` directory are treated as applicable for all CDR types. If you need a type specific check, place it in `qa_checks/<cdr_type>`.

!!!note
    
    To be a valid QA check, your select statement should return a single value.

When writing your QA check, you're almost certain to need to be able to refer to the table of data that's just been loaded. Because the SQL file will be [templated](https://airflow.apache.org/docs/stable/concepts.html#id1), you can use a macro which will be filled in when the check is run. The macro for the final table is `{{ final_table }}`.

Here's an example of a valid QA check; one of the defaults which records the number of just-added rows:

```sql
SELECT COUNT(*) FROM {{ final_table }}
```