Title: ETL

## Working with FlowETL

### Creating ETL pipelines

### Customising ETL pipelines

## Connecting to different CDR data sources

### Remote PostgreSQL database

### Remote Oracle database

### Remote MSSQL database

### CSV Files

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
| `{{ cdr_type }}` | The category of CDR data being loaded | `"calls"` |
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