from prefect import Flow, Parameter
from pathlib import Path
from datetime import datetime
import tasks

with Flow("Generate report") as flow:
    execution_timestamp = tasks.get_timestamp()
    
    # Some of these should be read from a config file, rather than passed as parameters
    output_notebooks_dir = Parameter("output_notebooks_dir")
    reports_dir = Parameter("reports_dir")
    run_flows_notebook = Parameter("run_flows_notebook")
    flows_report_notebook = Parameter("flows_report_notebook")
    api_url = Parameter("api_url")
    aggregation_unit = Parameter("aggregation_unit")
    anchor_date = Parameter("anchor_date")
    date_stencil = Parameter("date_stencil")
    asciidoc_template_file = Parameter("asciidoc_template_file")
    
    run_flows_output_notebook = tasks.papermill_execute_notebook(
        input_path=run_flows_notebook,
        output_dir=output_notebooks_dir,
        output_label=execution_timestamp,
        parameters=dict(api_url=api_url, aggregation_unit=aggregation_unit, anchor_date=anchor_date, date_stencil=date_stencil)
    )
    
    flows_report_output_notebook = tasks.papermill_execute_notebook(
        input_path=flows_report_notebook,
        output_dir=output_notebooks_dir,
        output_label=execution_timestamp,
        parameters=dict(api_url=api_url, aggregation_unit=aggregation_unit, anchor_date=anchor_date, previous_notebook=run_flows_output_notebook)
    )
    
    flows_report_pdf = tasks.convert_notebook_to_pdf(
        notebook_path=flows_report_output_notebook,
        output_dir=reports_dir,
        asciidoc_template_file=asciidoc_template_file,
    )