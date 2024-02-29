from dataclasses import asdict, dataclass
from datetime import date, datetime
from itertools import product
from pathlib import Path
from typing import Generator
from jinja2 import Environment, PackageLoader, Template
from sqlalchemy import create_engine
import os

env = Environment(loader=PackageLoader("flowetl", "qa_checks/qa_checks"))

db_user = os.getenv("POSTGRES_USER", "flowdb")
db_name = os.getenv("POSTGRES_DB", "flowdb")
db_port = os.getenv("POSTGRES_PORT", "9000")
db_password = os.getenv("POSTGRES_PASSWORD", "flowflow")  # In here for dev, don't think
conn_str = f"postgresql://{db_user}:{db_password}@localhost:{db_port}/{db_name}"
engine = create_engine(conn_str)


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
    dates: list[date]
    tables: list[str]


# NOTE: given this gets run _after_ all the ingestion, 'dates' may be an irrelevence
# unless final_table is normally one of the dated partitions
def render_qa_check(template: Template, date: date, cdr_type: str) -> str:
    return template.render(
        final_table=f"events.{cdr_type}",
        cdr_type=cdr_type,
        ds=date.strftime("%Y-%m-%d"),
    )


qa_scn = MockQaScenario(dates=[date(2016, 1, 1), date(2016, 1, 2)], tables=["calls"])

templates = (
    QaTemplate(Path(t).name, env.get_template(t)) for t in env.list_templates(".sql")
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
