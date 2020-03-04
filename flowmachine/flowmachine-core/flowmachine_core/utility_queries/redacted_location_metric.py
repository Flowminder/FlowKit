# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from typing import List


class RedactedLocationMetric:
    """
    Mixin which provides a redacted version of a query. Must be mixed into a class
    which sets the `redaction_target` attribute to the query to be redacted, and the
    redaction target _must_ have a value column which corresponds to a count of subscribers.
    """

    @property
    def column_names(self) -> List[str]:
        return self.redaction_target.column_names

    def _make_query(self):

        sql = f"""
        SELECT
            {self.column_names_as_string_list}
        FROM
            ({self.redaction_target.get_query()}) AS agged
        WHERE agged.value > 15
        """

        return sql
