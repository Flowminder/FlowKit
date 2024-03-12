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
from sqlalchemy.engine import Engine
import os
import argparse


env = Environment(loader=PackageLoader("flowetl", "qa_checks/qa_checks"))

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


def render_qa_check(template: Template, date: date, cdr_type: str) -> str:
    return template.render(
        final_table=f"events.{cdr_type}",
        cdr_type=cdr_type,
        ds=date.strftime("%Y-%m-%d"),
    )


def get_available_tables(engine: Engine):
    with engine.begin() as conn:
        tables = conn.execute("SELECT table_name FROM available_tables")
        return tables.all()


def get_available_dates(engine: Engine):
    with engine.begin() as conn:
        dates = conn.execute("SELECT cdr_date FROM etl.available_dates")
        return dates.all()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Runs all flowetl checks for ingested data"
    )
    parser.add_argument(
        "--dates",
        type=lambda s: datetime.datetime.strptime(s, "%Y-%m-%d"),
        help="Date to run ingestion check on. Can be specified multiple times.",
        nargs="*",
    )
    parser.add_argument(
        "--event-types", help="Event tables to run qa checks on.", nargs="*"
    )
    args = parser.parse_args()

    db_user = os.environ["POSTGRES_USER"]
    db_password = os.environ["POSTGRES_PASSWORD"]
    conn_str = f"postgresql://{db_user}:{db_password}@localhost:5432/flowdb"
    engine = create_engine(conn_str)

    breakpoint()
    if "dates" not in args.keys():
        args["dates"] = get_available_dates(engine)

    if "event-types" not in args.keys():
        args["event-types"] = get_available_tables(engine)

    qa_scn = MockQaScenario(dates=args.dates, tables=args.tables)

    templates = (
        QaTemplate(Path(t).name, env.get_template(t))
        for t in env.list_templates(".sql")
    )

    qa_rows = (
        QaRow(
            date,
            cdr_type,
            template.display_name,
            render_qa_check(template.template, date, cdr_type),
            "Made from mock data",
            datetime.now(),
        )
        for date, cdr_type, template in product(qa_scn.dates, qa_scn.tables, templates)
    )

    with engine.begin() as conn:
        for row in qa_rows:
            conn.execute(update_template.render(**asdict(row)))

        out = conn.execute("SELECT * FROM etl.post_etl_queries")
        print(out.fetchall())
