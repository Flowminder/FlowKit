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


class QueryLoadError(Exception):
    """
    Exception which indicates an error while loading a query object from a dict of parameters.

    Parameters
    ----------
    error_msg : str
        Human-readable string describing the error that occurred.
    params : dict
        Query parameters that could not be deserialised.
    **kwargs
        Additional details relating to the error (e.g. validation errors).

    Attributes
    ----------
    error_msg : str
        Human-readable string describing the error that occurred.
    params : dict
        Query parameters that could not be deserialised.
    details : dict
        Dict of additional details relating to the error (e.g. validation errors).

    """

    def __init__(self, error_msg: str, params: dict, **kwargs):
        super().__init__(error_msg)
        self.error_msg = error_msg
        self.params = params
        self.details = kwargs
