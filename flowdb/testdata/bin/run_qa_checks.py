# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from dataclasses import asdict, dataclass
from datetime import date, datetime
from itertools import product
from pathlib import Path
from typing import List
from jinja2 import Environment, PackageLoader, Template
from sqlalchemy import create_engine
import os
import argparse


env = Environment(loader=PackageLoader("flowetl", "qa_checks/qa_checks"))

# @James; if something like this already exists, I'll grab
# that instead
update_template_string = """
INSERT INTO etl.post_etl_queries
    (cdr_date, cdr_type, type_of_query_or_check, outcome, optional_comment_or_description, timestamp)
VALUES(
    '{{cdr_date}}',
    '{{cdr_type}}',
    '{{type_of_query_or_check}}',
    ({{outcome_query}}),
    '{{optional_comment_or_description}}',
    '{{timestamp}}'
)

"""

update_template = env.from_string(update_template_string)


@dataclass
class QaTemplate:
    display_name: str
    template: Template


@dataclass
class QaRow:
    cdr_date: date
    cdr_type: str
    type_of_query_or_check: str
    outcome_query: str
    optional_comment_or_description: str
    timestamp: datetime


@dataclass
class MockQaScenario:
    dates: List[date]
    tables: List[str]


# NOTE: given this gets run _after_ all the ingestion, 'dates' may be an irrelevence
# unless final_table is normally one of the dated partitions
def render_qa_check(template: Template, date: date, cdr_type: str) -> str:
    return template.render(
        final_table=f"events.{cdr_type}",
        cdr_type=cdr_type,
        ds=date.strftime("%Y-%m-%d"),
    )


if __name__ == "__main__()":
    parser = argparse.ArgumentParser(
        description="Runs all flowetl checks for ingested data"
    )
    parser.add_argument(
        "--dates",
        type=lambda s: datetime.datetime.strptime(s, "%Y-%m-%d"),
        help="Date to run ingestion check on. Can be specified multiple times.",
        nargs="+",
    )
    parser.add_argument(
        "--event-types", help="Event tables to run qa checks on.", nargs="+"
    )
    args = parser.parse_args()

    db_user = os.getenv("POSTGRES_USER", "flowdb")
    db_name = os.getenv("POSTGRES_DB", "flowdb")
    db_port = os.getenv("POSTGRES_PORT", "9000")
    db_password = os.getenv(
        "POSTGRES_PASSWORD", "flowflow"
    )  # In here for dev, don't think we'll need it in prod
    conn_str = f"postgresql://{db_user}:{db_password}@localhost:{db_port}/{db_name}"
    engine = create_engine(conn_str)

    qa_scn = MockQaScenario(dates=args.dates, tables=args.tables)

    templates = (
        QaTemplate(Path(t).name, env.get_template(t))
        for t in env.list_templates(".sql")
    )

    qa_rows = (
        QaRow(
            date,
            type,
            template.display_name,
            render_qa_check(template.template, date, type),
            "Made from mock data",
            datetime.now(),
        )
        for date, type, template in product(qa_scn.dates, qa_scn.tables, templates)
    )

    with engine.connect() as conn:
        for row in qa_rows:
            conn.execute(update_template.render(**asdict(row)))

        out = conn.execute("SELECT * FROM etl.post_etl_queries")
        print(out.fetchall())
