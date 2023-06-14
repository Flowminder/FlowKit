# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

import warnings

from datetime import datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Callable, Dict, Iterable, List, Optional, Union

from pendulum import Duration


class FluxSensorType(Enum):
    """
    Valid flux sensor types
    """

    FILE: str = "file"
    TABLE: str = "table"
    NOCHECK: str = "no_check"


def get_qa_checks(
    *,
    dag: Optional["DAG"] = None,
    additional_qa_check_paths: Optional[List[str]] = None,
) -> List["QACheckOperator"]:
    """
    Create from .sql files a list of QACheckOperators which are applicable for this dag.
    Adds all the 'default' checks from this package (see the qa_checks module), any
    found under <dag_folder>/qa_checks or additionally added template search paths, and any found
    under additional_qa_check_paths.

    CDR type-specific QA checks found under qa_checks/<cdr_type> will also be added if
    they match the CDR type set for the dag.

    Parameters
    ----------
    dag : DAG
        The DAG to add operators to. May be None, if called within a DAG context manager.
    additional_qa_check_paths : list of str
        Additional fully qualified paths to search for qa checks

    Returns
    -------
    list of QACheckOperator
    """
    from flowetl.operators.qa_check_operator import QACheckOperator
    from airflow.models.dag import DagContext
    from airflow import settings

    if dag is None:
        dag = DagContext.get_current_dag()
    if dag is None:
        raise TypeError("Must set dag argument or be in a dag context manager.")

    # The issue is that the file folder is getting added to the search path
    # here, this will be this file's parent
    # but later, it will be the dags folder by the look of it
    # even worse, airflow is actually setting this to the folder containing the file
    # of the caller of this function, so we want to use that if called from this file
    # and otherwise explicitly add the qa checks dir?
    default_path = (
        dag.folder
        if dag.fileloc == str(Path(__file__))
        else Path(__file__).parent / "qa_checks"
    )
    dag.template_searchpath = [
        *(additional_qa_check_paths if additional_qa_check_paths is not None else []),
        *(dag.template_searchpath if dag.template_searchpath is not None else []),
        default_path,  # Contains the default checks
        settings.DAGS_FOLDER,
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


def choose_flux_sensor(
    use_flux_sensor: Union[bool, str], is_file_dag: bool
) -> FluxSensorType:
    """
    Choose which flux sensor to use.

    Parameters
    ----------
    use_flux_sensor : bool or str
        Options:

        - 'file': check the last modification time of the file,
        - 'table': check the number of rows in the source table,
        - True: use 'file' flux sensor when extracting from a file, or 'table' flux sensor when extracting from a table within the database,
        - False: skip the flux check entirely.

    is_file_dag : bool
        True if loading from a file, or False if extracting from a table within the database

    Returns
    -------
    FluxSensorType
        Type of flux check to use
    """
    if isinstance(use_flux_sensor, str):
        flux_sensor_type = FluxSensorType(use_flux_sensor)
    elif use_flux_sensor:
        flux_sensor_type = (
            FluxSensorType("file") if is_file_dag else FluxSensorType("table")
        )
    else:
        flux_sensor_type = FluxSensorType("no_check")

    if flux_sensor_type == FluxSensorType.FILE and not is_file_dag:
        raise ValueError("File flux sensor can only be used when loading from a file.")

    return flux_sensor_type


def create_dag(
    *,
    dag_id: str,
    cdr_type: str,
    start_date: datetime,
    extract_sql: str,
    end_date: Optional[datetime] = None,
    retries: int = 10,
    retry_delay: timedelta = timedelta(days=1),
    schedule_interval: Union[str, Duration] = "@daily",
    indexes: Iterable[str] = ("msisdn_counterpart", "location_id", "datetime", "tac"),
    data_present_poke_interval: int = 60,
    data_present_timeout: int = 60 * 60 * 24 * 7,
    flux_check_poke_interval: int = 60,
    flux_check_wait_interval: int = 60,
    flux_check_timeout: int = 60 * 60 * 24 * 7,
    use_flux_sensor: Union[bool, str] = True,
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
    additional_qa_check_paths: Optional[List[str]] = None,
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
    schedule_interval : str or Duration, default "@daily"
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
    use_flux_sensor : bool or str, default True
        Set to 'file' to use a check on the last modification time of the file to determine
        whether the file is in flux (only available when loading data from a file). Set to 'table'
        to perform a slower check based on the number of rows in the mounted table, or set to False
        to skip the flux check entirely. Default (True) is to use 'file' flux sensor when extracting from a file,
        or 'table' flux sensor when extracting from a table within the database.
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
    additional_qa_check_paths : list of str
        Additional fully qualified paths to search for qa checks

    Returns
    -------
    DAG

    """

    from airflow import DAG
    from airflow.operators.latest_only import LatestOnlyOperator
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
    from flowetl.operators.update_location_ids_table_operator import (
        UpdateLocationIDsTableOperator,
    )
    from flowetl.sensors.data_present_sensor import DataPresentSensor
    from flowetl.sensors.file_flux_sensor import FileFluxSensor
    from flowetl.sensors.table_flux_sensor import TableFluxSensor

    # Choose flux sensor
    flux_sensor_type = choose_flux_sensor(use_flux_sensor, filename is not None)

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
                task_id="create_staging_view", sql=staging_view_sql
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

        create_staging_view >> check_not_empty

        if flux_sensor_type == FluxSensorType.FILE:
            check_not_in_flux = FileFluxSensor(
                task_id="check_not_in_flux",
                filename=filename,
                mode="reschedule",
                poke_interval=flux_check_poke_interval,
                flux_check_interval=flux_check_wait_interval,
                timeout=flux_check_timeout,
            )
            check_not_empty >> check_not_in_flux >> extract
        elif flux_sensor_type == FluxSensorType.TABLE:
            check_not_in_flux = TableFluxSensor(
                task_id="check_not_in_flux",
                mode="reschedule",
                poke_interval=flux_check_poke_interval,
                flux_check_interval=flux_check_wait_interval,
                timeout=flux_check_timeout,
            )
            check_not_empty >> check_not_in_flux >> extract
        else:
            check_not_empty >> extract

        add_constraints = AddConstraintsOperator(
            task_id="add_constraints", pool="postgres_etl"
        )
        add_indexes = CreateIndexesOperator(
            task_id="add_indexes", index_columns=indexes, pool="postgres_etl"
        )
        attach = AttachOperator(task_id="attach")
        analyze = AnalyzeOperator(
            task_id="analyze", target="{{ extract_table }}", pool="postgres_etl"
        )
        latest_only = LatestOnlyOperator(task_id="analyze_parent_only_for_new")
        analyze_parent = AnalyzeOperator(
            task_id="analyze_parent", target="{{ parent_table }}", pool="postgres_etl"
        )
        update_records = UpdateETLTableOperator(task_id="update_records")
        update_location_ids_table = UpdateLocationIDsTableOperator(
            task_id="update_location_ids_table"
        )

        from_stage = extract

        if cluster_field is not None:
            cluster = ClusterOperator(
                task_id="cluster", cluster_field=cluster_field, pool="postgres_etl"
            )
            extract >> cluster
            from_stage = cluster
        (
            from_stage
            >> [
                add_constraints,
                add_indexes,
            ]
            >> analyze
            >> attach
            >> latest_only
            >> analyze_parent
        )
        attach >> [
            update_records,
            update_location_ids_table,
            *get_qa_checks(additional_qa_check_paths=additional_qa_check_paths),
        ]
    globals()[dag_id] = dag
    return dag
