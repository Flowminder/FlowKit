import datetime as dt
import re
from contextlib import contextmanager
from pathlib import Path
import psycopg2
import os

def _parse_date(date):
    if type(date) == dt.date:
        return dt.date.strftime("%Y_%m_%d")
    if type(date) == str:
        try:
            dt.datetime.strptime(date, "%Y_%m_%d")
        except ValueError:
            raise ValueError("Date should be in format yyyy_mm_dd")
        return date


@contextmanager
def cd(path):
    old_dir = os.getcwd()
    try:
        os.chdir(path)
        yield
    finally:
        os.chdir(old_dir)

@contextmanager
def get_cursor(db_con):
    cur = db_con.cursor()
    try:
        yield cur
    finally:
        cur.close()

class ArchiveManager:

    def __init__(self, archive_dir):
        # TODO: path validation, input validation
        self.archive_dir = Path(archive_dir).absolute().__str__()
        self.db_con = psycopg2.connect(
            host = os.getenv("FLOWDB_HOST"),
            port = os.getenv("FLOWDB_PORT"),
            user = os.getenv("POSTGRES_USER"),
            password = os.getenv("POSTGRES_PASSWORD"),
            dbname = "flowdb"
        )

    def csv_to_flowdb(self, date):
        date = _parse_date(date)
        with cd(Path(__file__).parent):
            with open("../../FlowKit/flowetl/flowetl/flowetl/archive/sql/create_and_fill_staging_table.sql", 'r') as staging_file:
                staging_query = staging_file.read().format(date = date, csv_dir = self.archive_dir)
            with open("../../FlowKit/flowetl/flowetl/flowetl/archive/sql/create_and_fill_reduced_table.sql", 'r') as reduced_file:
                reduced_query = reduced_file.read().format(date = date)
        with get_cursor(self.db_con) as cur:
            cur.execute(staging_query)
            cur.execute(reduced_query)
