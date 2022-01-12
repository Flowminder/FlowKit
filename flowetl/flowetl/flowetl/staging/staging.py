import datetime as dt
import re
from contextlib import contextmanager
from pathlib import Path
from typing import Union

import psycopg2
import os

import logging

logger = logging.getLogger("flowdb")


def _parse_date(date):
    if type(date) == dt.date:
        return dt.date.strftime(date, "%Y_%m_%d")
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


# TODO: Full rebuild query (reduced table, cell table, geography table)

# Make this an ABC?
class StagingStep:
    def __init__(self, query_path: Union[os.PathLike, str], query_args=None):
        if query_args is None:
            query_args = {}
        self.query_path = str(Path(query_path))
        self.query_args = query_args
        self.query_name = Path(query_path).name
        with open(query_path, "r") as query_file:
            logger.info(f"Loading {self.query_name} with args {self.query_args}")
            try:
                self.query = query_file.read().format(**self.query_args)
            except KeyError as ke:
                logger.critical(f"{self.query_name} requires arg {ke}")
                raise KeyError from ke

    def execute(self, cursor):
        logger.info(f"Running {self.query_name} with args {self.query_args}")
        return cursor.execute(self.query)


class FillStagingTableStep(StagingStep):
    def __init__(self, query_args):
        with cd(Path(__file__).parent):
            super().__init__("sql/create_and_fill_staging_table.sql", query_args)


class OptOutStep(StagingStep):
    def __init__(self, query_args):
        with cd(Path(__file__).parent):
            super().__init__("sql/opt_out.sql", query_args)


class ReduceStep(StagingStep):
    def __init__(self, query_args):
        with cd(Path(__file__).parent):
            super().__init__(
                "./sql/create_and_fill_day_sightings_table.sql", query_args
            )


class ExampleMappingStep(StagingStep):
    def __init__(self, query_args):
        with cd(Path(__file__).parent):
            super().__init__("./sql/example_location_mapping.sql", query_args)


class DefaultMappingStep(StagingStep):
    def __init__(self, query_args):
        with cd(Path(__file__).parent):
            super().__init__("./sql/default_location_mapping.sql", query_args)


class ApplyMappingToEvents(StagingStep):
    def __init__(self, query_args):
        with cd(Path(__file__).parent):
            super().__init__("./sql/apply_mapping_to_staged_events.sql", query_args)


# Question; where to put query_args?
# My first thought was they get supplied in the construction of these classes.
# The rationale behind this class design being that we can compose strings of
# ingestion processes for different scenarios. This also lets us keep a continue
# point should an ingestion process become interrupted (to be implemented)


class StagingManager:
    def __init__(
        self,
        archive_dir,
        opt_out_list_path: Union[os.PathLike, str] = None,
        tower_clustering_class=DefaultMappingStep,
    ):
        # TODO: path validation, input validation
        self.archive_dir = Path(archive_dir).absolute().__str__()
        self.opt_out_path = Path(opt_out_list_path).absolute().__str__()
        self.tower_clustering_class = tower_clustering_class
        self.db_con = psycopg2.connect(
            host=os.getenv("FLOWDB_HOST"),
            port=os.getenv("FLOWDB_PORT"),
            user=os.getenv(
                "POSTGRES_USER"
            ),  # Replace with lesss expansive permissions later
            password=os.getenv("POSTGRES_PASSWORD"),
            dbname="flowdb",
        )
        self.query_args = {
            "csv_dir": self.archive_dir,
            "opt_out_path": self.opt_out_path,
        }

    def load_csv_on_date(self, date):
        date = _parse_date(date)
        self.query_args["date"] = date

        with get_cursor(self.db_con) as cur:
            FillStagingTableStep(self.query_args).execute(cur)
            if self.opt_out_path is not None:
                OptOutStep(self.query_args).execute(cur)

            self.tower_clustering_class(self.query_args).execute(cur)
            ApplyMappingToEvents(self.query_args).execute(cur)

            ReduceStep(self.query_args).execute(cur)
