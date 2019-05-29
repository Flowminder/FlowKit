# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.


class FlowmachineServerError(Exception):
    """
    Exception which indicates an error during zmq message processing.
    """

    def __init__(self, error_msg):
        super().__init__(error_msg)
        self.error_msg = error_msg
