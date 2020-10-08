# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Union

from pendulum import Interval


def get_qa_checks(*, dag: Optional["DAG"] = None) -> List["QACheckOperator"]:
    """
    Create from .sql files a list of QACheckOperators which are applicable for this dag.
    Adds all the 'default' checks from this package (see the qa_checks module), and any
    found under <dag_folder>/qa_checks or additionally added template search paths.

    CDR type-specific QA checks found under qa_checks/<cdr_type> will also be added if
    they match the CDR type set for the dag.

    Parameters
    ----------
    dag : DAG
        The DAG to add operators to. May be None, if called within a DAG context manager.

    Returns
    -------
    list of QACheckOperator
    """
    from flowetl.operators.qa_check_operator import QACheckOperator
    from airflow import settings

    if dag is None:
        dag = settings.CONTEXT_MANAGER_DAG
    if dag is None:
        raise TypeError("Must set dag argument or be in a dag context manager.")
    # Add the default QA checks to the template path
    default_checks = Path(__file__).parent / "qa_checks"
    dag.template_searchpath = [
        *(dag.template_searchpath if dag.template_searchpath is not None else []),
        str(default_checks),
    ]
    jinja_env = dag.get_template_env()
    templates = [
        Path(tmpl)
        for tmpl in jinja_env.list_templates(
            filter_func=lambda tmpl: "qa_checks" in tmpl and tmpl.endswith(".sql")
        )
    ]
    valid_stems = (
        "qa_checks",
        *((dag.params["cdr_type"],) if "cdr_type" in dag.params else ()),
    )
    template_paths = [tmpl for tmpl in templates if tmpl.parent.stem in valid_stems]

    return [
        QACheckOperator(
            task_id=tmpl.stem
            if tmpl.parent.stem == "qa_checks"
            else f"{tmpl.stem}.{tmpl.parent.stem}",
            sql=str(tmpl),
            dag=dag,
        )
        for tmpl in sorted(template_paths)
    ]


def create_dag(
    *,
    dag_id: str,
    cdr_type: str,
    start_date: datetime,
    extract_sql: str,
    end_date: Optional[datetime] = None,
    retries: int = 10,
    retry_delay: timedelta = timedelta(days=1),
    schedule_interval: Union[str, Interval] = "@daily",
    indexes: Iterable[str] = ("msisdn_counterpart", "location_id", "datetime", "tac"),
    data_present_poke_interval: int = 60,
    data_present_timeout: int = 60 * 60 * 24 * 7,
    flux_check_poke_interval: int = 60,
    flux_check_wait_interval: int = 60,
    flux_check_timeout: int = 60 * 60 * 24 * 7,
    source_table: Optional[str] = None,
    staging_view_sql: Optional[str] = None,
    cluster_field: Optional[str] = None,
    program: Optional[str] = None,
    filename: Optional[str] = None,
    fields: Optional[Dict[str, str]] = None,
    null: str = "",
    additional_macros: Dict[str, Union[str, Callable]] = dict(),
    header: bool = True,
    delimiter: str = ",",
    quote: str = '"',
    escape: str = '"',
    encoding: Optional[str] = None,
    use_file_flux_sensor: bool = True,
    **kwargs,
) -> "DAG":
    """
    Create an ETL DAG that will load data from files, or a table within the database.

    Parameters
    ----------
    dag_id : str
        Name of the dag
    cdr_type : {"calls", "sms", "mds", "topups"}
        Type of CDR data
    start_date : datetime
        First date the dag should run for
    extract_sql : str
        SQL template. May be an SQL string, or the name of a file in the dags folder. The SQL should output
        a table with fields matching the corresponding cdr type schema. Where the source data is missing a
        field, the field must be introduced using NULL::<field_type> as <field_name>.
    end_date : datetime or None
        Optionally specify the final day the day should run on
    retries : int, default 10
        Number of times to retry the dag if it fails
    retry_delay : timedelta, default timedelta(days=1)
        Delay between retries
    schedule_interval : str or Interval, default "@daily"
        Time interval between execution dates.
    indexes : iterable of str, default ("msisdn_counterpart", "location_id", "datetime", "tac")
        Fields to create indexes on.
    data_present_poke_interval : int, default 60
        Number of seconds to wait between runs for the data present check
    data_present_timeout : int, default 604800
        Maximum number of seconds to keep checking before failing
    flux_check_poke_interval : int, default 60
        Number of seconds to wait between runs for the data in flux check
    flux_check_wait_interval : int, default 60
        Number of seconds to monitor data when checking for flux
    flux_check_timeout : int, default 604800
        Maximum number of seconds to keep checking before failing
    source_table : str or None
        If extracting from a table within the database (e.g. when using a FDW to connect to another db),
        the schema qualified name of the table.
    staging_view_sql : str or None
        If extracting from a table within the database (e.g. when using a FDW to connect to another db), the sql template
        or name of the template which will be used to create a date limited view of the data.
    cluster_field : str or None
        Optionally require that the data tables be 'clustered' on a field, which improves the performance of queries
        which need to subset based on that field at the cost of a significant increase in ETL time.
    program : str or None
        When loading data from files, set to the name of a program to be used when reading them (e.g. zcat to load
        from compressed csv files).
    filename : str or None
        When loading data from files, the filename pattern to be used - may include Airflow macros.
    fields : dict or None
        When loading data from files, a mapping of field names to postgres types.
    null : str, default ""
        When loading data from files, optionally specify a null value character
    additional_macros : dict or None
        Optionally provide additional macros to be available in SQL templates.
    header : bool, default True
        Set to False when loading files if the files do not have a header row.
    delimiter : str, default ","
        When loading from files, you may specify the delimiter character
    quote : str, default '"'
        When loading from files, you may specify the quote character
    escape : str, default '"'
        When loading from files, you may specify the escape character
    encoding : str or None
        Optionally specify file encoding when loading from files.
    use_file_flux_sensor : bool, default True
        When set to True, uses a check on the last modification time of the file to determine
        whether the file is in flux.  Set to False to perform a slower check based on the
        number of rows in the mounted table.

    Returns
    -------
    DAG

    """

    from airflow import DAG
    from airflow.operators.latest_only_operator import LatestOnlyOperator
    from flowetl.operators.add_constraints_operator import AddConstraintsOperator
    from flowetl.operators.analyze_operator import AnalyzeOperator
    from flowetl.operators.attach_operator import AttachOperator
    from flowetl.operators.cluster_operator import ClusterOperator
    from flowetl.operators.create_foreign_staging_table_operator import (
        CreateForeignStagingTableOperator,
    )
    from flowetl.operators.create_indexes_operator import CreateIndexesOperator
    from flowetl.operators.create_staging_view_operator import CreateStagingViewOperator
    from flowetl.operators.extract_from_foreign_table_operator import (
        ExtractFromForeignTableOperator,
    )
    from flowetl.operators.extract_from_view_operator import ExtractFromViewOperator
    from flowetl.operators.update_etl_table_operator import UpdateETLTableOperator
    from flowetl.sensors.data_present_sensor import DataPresentSensor
    from flowetl.sensors.file_flux_sensor import FileFluxSensor
    from flowetl.sensors.table_flux_sensor import TableFluxSensor

    args = {
        "owner": "airflow",
        "retries": retries,
        "retry_delay": retry_delay,
        "postgres_conn_id": "flowdb",
        "conn_id": "flowdb",
        "start_date": start_date,
        "end_date": end_date,
        **kwargs,
    }

    macros = dict(**additional_macros)
    if source_table is not None:
        macros["source_table"] = source_table

    with DAG(
        dag_id=dag_id,
        schedule_interval=schedule_interval,
        default_args=args,
        user_defined_macros=macros,
        params=dict(cdr_type=cdr_type),
    ) as dag:
        if staging_view_sql is not None and source_table is not None:
            create_staging_view = CreateStagingViewOperator(
                task_id="create_staging_view",
                sql=staging_view_sql,
            )
            extract = ExtractFromViewOperator(
                task_id="extract", sql=extract_sql, pool="postgres_etl"
            )
        elif filename is not None and len(fields) > 0:
            create_staging_view = CreateForeignStagingTableOperator(
                task_id="create_staging_view",
                program=program,
                filename=filename,
                fields=fields,
                null=null,
                header=header,
                delimiter=delimiter,
                quote=quote,
                escape=escape,
                encoding=encoding,
            )
            extract = ExtractFromForeignTableOperator(
                task_id="extract", sql=extract_sql, pool="postgres_etl"
            )
        else:
            raise TypeError(
                "Either staging_view_sql and source_table, or filename and fields must be provided."
            )
        check_not_empty = DataPresentSensor(
            task_id="wait_for_data",
            mode="reschedule",
            poke_interval=data_present_poke_interval,
            timeout=data_present_timeout,
        )
        if filename is not None and use_file_flux_sensor:
            check_not_in_flux = FileFluxSensor(
                task_id="check_not_in_flux",
                filename=filename,
                mode="reschedule",
                poke_interval=flux_check_poke_interval,
                flux_check_interval=flux_check_wait_interval,
                timeout=flux_check_timeout,
            )
        else:
            check_not_in_flux = TableFluxSensor(
                task_id="check_not_in_flux",
                mode="reschedule",
                poke_interval=flux_check_poke_interval,
                flux_check_interval=flux_check_wait_interval,
                timeout=flux_check_timeout,
            )

        add_constraints = AddConstraintsOperator(
            task_id="add_constraints", pool="postgres_etl"
        )
        add_indexes = CreateIndexesOperator(
            task_id="add_indexes",
            index_columns=indexes,
            pool="postgres_etl",
        )
        attach = AttachOperator(task_id="attach")
        analyze = AnalyzeOperator(
            task_id="analyze",
            target="{{ extract_table }}",
            pool="postgres_etl",
        )
        latest_only = LatestOnlyOperator(task_id="analyze_parent_only_for_new")
        analyze_parent = AnalyzeOperator(
            task_id="analyze_parent",
            target="{{ parent_table }}",
            pool="postgres_etl",
        )
        update_records = UpdateETLTableOperator(task_id="update_records")

        create_staging_view >> check_not_empty >> check_not_in_flux >> extract
        from_stage = extract

        if cluster_field is not None:
            cluster = ClusterOperator(
                task_id="cluster", cluster_field=cluster_field, pool="postgres_etl"
            )
            extract >> cluster
            from_stage = cluster
        from_stage >> [
            add_constraints,
            add_indexes,
        ] >> analyze >> attach >> latest_only >> analyze_parent
        attach >> [update_records, *get_qa_checks()]
    globals()[dag_id] = dag
    return dag
