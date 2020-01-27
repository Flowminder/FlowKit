Title: ETL

## Working with FlowETL

FlowETL manages the loading of CDR data into FlowDB. It is built on [Apache Airflow](https://airflow.apache.org), and a basic understanding of how to use Airflow will be very helpful in making the best use of FlowETL. We recommend you familiarise yourself with the Airflow [tutorial](https://airflow.apache.org/docs/stable/tutorial.html), and [key concepts](https://airflow.apache.org/docs/stable/concepts.html) before continuing.

### Creating ETL pipelines

### Customising ETL pipelines

## Connecting to different CDR data sources

### Remote PostgreSQL database

### Remote Oracle database

### Remote MSSQL database

### CSV Files

FlowETL can also be used to load data from files - useful if you're receiving data as a daily file dump. To load from a file, you'll need to ensure that the files have a predictable date based name, for example `calls_data_2019_05_01.csv.gz`, which you can capture using a ([templated](https://airflow.apache.org/docs/stable/concepts.html#id1)) string. The filename pattern should include the absolute path to the files from _inside_ your FlowDB container, for example a complete pattern to capture files with names like `calls_data_2019-05-01.csv.gz` in the `/etl/calls` data root might be `/etl/{{ params.cdr_type }}/{{ params.cdr_type }}_data_{{ ds }}`. This uses a combination of Airflow's [built-in macros](https://airflow.apache.org/docs/stable/macros.html#default-variables), and the `{{ params.cdr_type }}` macro supplied by FlowETL.

You will also need to specify the names and [types](https://www.postgresql.org/docs/current/datatype.html) of the fields your CSV will contain as a dict. These will be used to create a [foreign data wrapper](https://www.postgresql.org/docs/current/file-fdw.html) which allows FlowDB to treat your data file like a table, and helps ensure data integrity. You may optionally specify a program to be run to read your data file, for example `zcat` if your source data is compressed. You can either pass these arguments to the [`create_dag`](../../../../flowetl/flowetl/util/#create_dag) function, or use [`CreateForeignStagingTableOperator`](../../../../flowetl/flowetl/operators/create_foreign_staging_table_operator) directly if composing your DAG manually. If you are composing the DAG manually, you will need to use the `ExtractFromForeignTableOperator` to ensure that FlowETL correctly handles cleanup of intermediary ETL steps.

## Loading GIS data

## Data QA checks

### Adding new QA checks

Adding additional QA checks is as simple as writing an SQL select statement. To add your own custom checks, create a `qa_checks` subfolder in the directory where you are keeping your dags. You can then create your check as a `<check name.sql>` file, and it will automatically get picked up for the next run of the dag.

SQL files under the `qa_checks` directory are treated as applicable for all CDR types. If you need a type specific check, place it in `qa_checks/<cdr_type>`.

!!!note
    
    To be a valid QA check, your select statement should return a single value.

When writing your QA check, you're almost certain to need to be able to refer to the table of data that's just been loaded. Because the SQL file will be [templated](https://airflow.apache.org/docs/stable/concepts.html#id1), you can use a macro which will be filled in when the check is run. The macro for the final table is `{{ final_table }}`, but there are several others:

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

Here's an example of a valid QA check; one of the defaults which records the number of just-added rows:

```sql
SELECT COUNT(*) FROM {{ final_table }}
```