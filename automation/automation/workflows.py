# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from prefect import Flow, Parameter, unmapped
from prefect.schedules import CronSchedule
from datetime import timedelta
from . import tasks

schedule = CronSchedule("0 0 * * *")

with Flow("Date-triggered-notebooks", schedule) as date_triggered_notebooks_workflow:
    # Parameters required in any date-triggered-notebooks workflow (should all have defaults)
    cdr_types = Parameter("cdr_types", default=None, required=False)
    earliest_date = Parameter("earliest_date", default=None, required=False)
    date_stencil = Parameter("date_stencil", default=None, required=False)
    asciidoc_template = Parameter("asciidoc_template", default=None, required=False)
    # Filenames of notebooks to run
    run_flows_notebook = Parameter("run_flows_notebook")
    flows_report_notebook = Parameter("flows_report_notebook")
    # Parameters to pass to notebooks (not used elsewhere in the workflow)
    aggregation_unit = Parameter("aggregation_unit")

    # Get FlowAPI URL, so that it can be passed as a parameter to notebook execution tasks.
    flowapi_url = tasks.get_flowapi_url()

    # Get list of reference dates for notebook runs
    all_dates = tasks.get_available_dates(cdr_types=cdr_types)
    dates_after_earliest = tasks.filter_dates_by_earliest_date(
        dates=all_dates, earliest_date=earliest_date
    )
    dates_with_available_stencil = tasks.filter_dates_by_stencil(
        dates=dates_after_earliest, available_dates=all_dates, date_stencil=date_stencil
    )
    reference_dates = tasks.filter_dates_by_previous_runs(dates_with_available_stencil)

    # Record each workflow run as 'in_process'
    in_process = tasks.record_workflow_in_process.map(reference_date=reference_dates)

    # Get date ranges and unique label for each reference date
    label = tasks.get_label.map(
        reference_date=reference_dates, upstream_tasks=[in_process]
    )
    date_ranges = tasks.get_date_ranges.map(
        reference_date=reference_dates, date_stencil=unmapped(date_stencil)
    )

    # Run notebooks for each reference date
    run_flows_output_notebook = tasks.papermill_execute_notebook.map(
        input_filename=unmapped(run_flows_notebook),
        output_label=label,
        parameters=tasks.mappable_dict.map(
            flowapi_url=unmapped(flowapi_url),
            aggregation_unit=unmapped(aggregation_unit),
            date_ranges=date_ranges,
        ),
    )
    flows_report_output_notebook = tasks.papermill_execute_notebook.map(
        input_filename=unmapped(flows_report_notebook),
        output_label=label,
        parameters=tasks.mappable_dict.map(
            flowapi_url=unmapped(flowapi_url),
            aggregation_unit=unmapped(aggregation_unit),
            reference_date=reference_dates,
            previous_notebook=run_flows_output_notebook,
        ),
    )

    # Create PDF report from flows_report notebook
    flows_report_pdf = tasks.convert_notebook_to_pdf.map(
        notebook_path=flows_report_output_notebook,
        asciidoc_template=unmapped(asciidoc_template),
    )

    # Record each successful workflow run as 'done'
    done = tasks.record_workflow_done.map(
        reference_date=reference_dates,
        upstream_tasks=[
            run_flows_output_notebook,
            flows_report_output_notebook,
            flows_report_pdf,
        ],
    )

    # Record any unsuccessful workflow runs as 'failed'
    tasks.record_workflows_failed(
        reference_dates=reference_dates, upstream_tasks=[in_process, done]
    )

date_triggered_notebooks_workflow.set_reference_tasks([done])
