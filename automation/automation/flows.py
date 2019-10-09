# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from prefect import Flow, Parameter, unmapped
from . import tasks

with Flow("Date-triggered-notebooks") as date_triggered_notebooks_workflow:
    # Parameters required in any date-triggered-notebooks workflow (should all have defaults)
    api_url = Parameter("api_url")  # TODO: set default here
    cdr_types = Parameter("cdr_types", default=None, required=False)
    earliest_date = Parameter("earliest_date", default=None, required=False)
    date_stencil = Parameter("date_stencil", default=None, required=False)
    asciidoc_template = Parameter("asciidoc_template")  # TODO: set default here
    # Filenames of notebooks to run
    run_flows_notebook = Parameter("run_flows_notebook")
    flows_report_notebook = Parameter("flows_report_notebook")
    # Parameters to pass to notebooks (not used elsewhere in the workflow)
    aggregation_unit = Parameter("aggregation_unit")

    # Get list of reference dates for notebook runs
    all_dates = tasks.get_available_dates(api_url=api_url, cdr_types=cdr_types)
    dates_after_earliest = tasks.filter_dates_by_earliest_date(
        dates=all_dates, earliest_date=earliest_date
    )
    dates_with_available_stencil = tasks.filter_dates_by_stencil(
        dates=dates_after_earliest, available_dates=all_dates, date_stencil=date_stencil
    )
    new_dates = tasks.filter_dates_by_previous_runs(dates_with_available_stencil)

    # Get date ranges and unique label for each reference date
    label = tasks.get_label.map(
        reference_date=new_dates,
        upstream_tasks=[
            tasks.record_workflow_in_process.map(
                reference_date=new_dates
            )  # Record that the workflow run has started before getting label
        ],
    )
    date_ranges = tasks.get_date_ranges.map(
        reference_date=new_dates, date_stencil=unmapped(date_stencil)
    )

    # Run notebooks for each reference date
    run_flows_output_notebook = tasks.papermill_execute_notebook.map(
        input_filename=unmapped(run_flows_notebook),
        output_label=label,
        parameters=tasks.mappable_dict.map(
            api_url=unmapped(api_url),
            aggregation_unit=unmapped(aggregation_unit),
            date_ranges=date_ranges,
        ),
    )
    flows_report_output_notebook = tasks.papermill_execute_notebook.map(
        input_filename=unmapped(flows_report_notebook),
        output_label=label,
        parameters=tasks.mappable_dict.map(
            api_url=unmapped(api_url),
            aggregation_unit=unmapped(aggregation_unit),
            reference_date=new_dates,
            previous_notebook=run_flows_output_notebook,
        ),
    )

    # Create PDF report from flows_report notebook
    flows_report_pdf = tasks.convert_notebook_to_pdf.map(
        notebook_path=flows_report_output_notebook,
        asciidoc_template=unmapped(asciidoc_template),
    )

    # Record whether each workflow run succeeded or failed
    tasks.record_workflow_done.map(
        reference_date=new_dates,
        upstream_tasks=[
            run_flows_output_notebook,
            flows_report_output_notebook,
            flows_report_pdf,
        ],
    )
    tasks.record_workflow_failed.map(
        reference_date=new_dates,
        upstream_tasks=[
            run_flows_output_notebook,
            flows_report_output_notebook,
            flows_report_pdf,
        ],
    )
