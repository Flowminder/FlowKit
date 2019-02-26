# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
# Versioned Infrastructure

Methods for fetching a set of versioned infrastructure elements.
A version is selected based on the date in which that version is valid.

"""
from typing import List

from datetime import datetime

from flowmachine.core import Table
from ...core.query import Query


class VersionedInfrastructure(Query):
    """
    Simple class that returns the set of infrastructure elements
    (either `sites` or `cells`) that are valid for a given
    date. This class uses those tables' versioning scheme
    to figure out what set of towers is valid.

    Parameters
    ----------

    table : str
        Infrastructure table to use. This can either be
        `sites` or `cells`. No other table is supported.

    date : str
        Date in ISO format `2016-01-22`. This is the
        date in which an infrastructure element is
        valid for. If no date is provided (i.e. None),
        the current date will be used (i.e. `datetime.now()`).
    """

    def __init__(self, table="sites", date=None):
        """
        Parameters
        ----------
        table: str, default 'sites'
            Which table to collection versioned information from.
            Only the tables infrastructure.sites and infrastructure.cells
            are supported.
        
        date: str, default None
            The date to collect a valid version from. This date
            must be formatted using ISO standards (2016-01-13).
            If no date is passed the current date will be used.
        
        """
        if table not in ("sites", "cells"):
            raise ValueError(
                "Only the tables infrastructure.sites and "
                + "and infrastructure.cells are supported."
            )

        if date == None:
            date = datetime.now().strftime("%Y-%m-%d")

        self.table = Table(schema="infrastructure", name=table)
        self.date = date

        super().__init__()

    @property
    def column_names(self) -> List[str]:
        return self.table.column_names

    def _make_query(self):

        sql = f"""
            {self.table.get_query()}
            WHERE date_of_first_service <= '{self.date}'::date AND
                  (CASE
                      WHEN date_of_last_service IS NOT NULL
                      THEN date_of_last_service > '{self.date}'::date
                      ELSE TRUE
                   END)
        """

        return sql
