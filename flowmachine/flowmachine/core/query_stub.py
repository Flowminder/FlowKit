# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

from flowmachine.core.query import Query


class QStub(Query):
    def __init__(self, deps, qid):
        self.deps = deps
        self._md5 = qid
        super().__init__()

    def _make_query(self):
        pass

    @property
    def column_names(self):
        pass
