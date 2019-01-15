# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

# -*- coding: utf-8 -*-
"""
Model is an abstract base class for models - features which
are calculated outside of the database.

In addition, it provides a decorator - model_result, which should
be used with the `run` method of a model. This will check for cached
results for the run parameters, and return the cache in preference to
running the model.


"""
import time
from abc import ABCMeta, abstractmethod
from functools import wraps

from .model_result import ModelResult


class Model(metaclass=ABCMeta):
    """
    Abstract base class for models.
    """

    @abstractmethod
    def run(self, *args, **kwargs):
        """
        Method which runs the model.

        Returns
        -------
        flowmachine.core.model_result.ModelResult
        """
        raise NotImplementedError

    @classmethod
    def get_stored(cls):
        """
        This method returns any previously stored model results.

        Returns
        -------
        Iterable of ModelResults of this class
        """
        return (
            res for res in ModelResult.get_stored() if res.parent_class == cls.__name__
        )


def model_result(f):
    """
    A decorator which creates a `ModelResult` object, and handles
    checking if the results of `run` are cached, returns them if so
    and converts the DataFrame returned by `run` into a `ModelResult`.

    Parameters
    ----------
    f : function
        run function to wrap

    Returns
    -------
    function

    """

    @wraps(f)
    def new_f(self, *args, **kwargs):
        mr = ModelResult(self, run_args=args, run_kwargs=kwargs)
        if mr.is_stored:
            return mr
        start_time = time.process_time()
        mr._df = f(self, *args, **kwargs)
        mr._runtime = time.process_time() - start_time
        return mr

    return new_f
