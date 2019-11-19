# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.

"""
Defines the DateStencil class, to represent a pattern of dates relative to a reference date.
"""

import datetime
from typing import Iterable, List, Optional, Sequence, Set, Tuple, Union

import pendulum


class InvalidDateIntervalError(ValueError):
    """
    Custom error to raise if a date stencil contains a date interval with start_date > end_date.
    """

    pass


class DateStencil:
    """
    A class that represents a sequence of date intervals, which can be a
    mixture of absolute dates and offsets relative to a reference date.

    Parameters
    ----------
    raw_stencil : sequence of date, int and/or pairs of date/int
        List of elements defining date intervals.
        Each element can be:
            - a date object corresponding to an absolute date,
            - an int corresponding to an offset (in days) relative to a reference date,
            - a length-2 sequence [start, end] of dates or offsets,
              corresponding to a date interval (inclusive of both limits).
    """

    def __init__(
        self,
        raw_stencil: Sequence[
            Union[Union[int, datetime.date], Sequence[Union[int, datetime.date]]]
        ],
    ):
        intervals = []
        for element in raw_stencil:
            if isinstance(element, (list, tuple)):
                if len(element) != 2:
                    raise ValueError(
                        "Expected date interval to have length 2 (in format [start, end]), "
                        f"but got sequence of length {len(element)}."
                    )
                self._validate_date_element(element[0])
                self._validate_date_element(element[1])
                if (
                    (isinstance(element[0], int) and isinstance(element[1], int))
                    or (
                        isinstance(element[0], datetime.date)
                        and isinstance(element[1], datetime.date)
                    )
                ) and element[0] > element[1]:
                    raise InvalidDateIntervalError(
                        f"Date stencil contains invalid interval ({element[0]}, {element[1]})."
                    )
                intervals.append((element[0], element[1]))
            else:
                self._validate_date_element(element)
                intervals.append((element, element))
        self._intervals = tuple(intervals)

    @staticmethod
    def _validate_date_element(element):
        if not isinstance(element, (int, datetime.date)):
            raise TypeError(f"{element} is not an integer or date.")

    @staticmethod
    def _offset_to_date(
        offset: Union[int, datetime.date], reference_date: datetime.date
    ) -> pendulum.Date:
        """
        Return a date corresponding to the offset from a reference date.

        Parameters
        ----------
        offset : int or date
            Either an integer number of days offset from reference date, or a date object.
            If a date object, this date will be returned.
        reference_date : date
            Date to calculate the offset relative to.

        Returns
        -------
        pendulum.Date
            reference_date + offset (if offset is an integer), or offset (if offset is a date).
        
        Raises
        ------
        TypeError
            If type(offset) is not either int or datetime.date
        """
        if isinstance(offset, datetime.date):
            date_from_offset = pendulum.date(offset.year, offset.month, offset.day)
        elif isinstance(offset, int):
            date_from_offset = pendulum.date(
                reference_date.year, reference_date.month, reference_date.day
            ).add(days=offset)
        else:
            raise TypeError(
                f"Invalid type for offset: expected 'date' or 'int', not '{type(offset).__name__}'."
            )
        return date_from_offset

    def as_date_pairs(
        self, reference_date: datetime.date
    ) -> List[Tuple[pendulum.Date, pendulum.Date]]:
        """
        Given a reference date to calculate the offsets relative to, return
        this date stencil as a list of tuples representing date intervals
        (inclusive of both limits).

        Parameters
        ----------
        reference_date : date
            Date to calculate offsets relative to.
        
        Returns
        -------
        list of tuple (pendulum.Date, pendulum.Date)
            List of pairs of date objects, each representing a date interval.
        
        Raises
        ------
        InvalidDateIntervalError
            If the stencil results in a date pair with start_date > end_date
        """
        date_pairs = []
        for element in self._intervals:
            start_date = self._offset_to_date(element[0], reference_date)
            end_date = self._offset_to_date(element[1], reference_date)
            if start_date > end_date:
                raise InvalidDateIntervalError(
                    f"Date stencil contains invalid date pair ({start_date}, {end_date}) for reference date {reference_date}."
                )
            date_pairs.append((start_date, end_date))
        return date_pairs

    def as_set_of_dates(self, reference_date: datetime.date) -> Set[pendulum.Date]:
        """
        Given a reference date to calculate the offsets relative to, return
        this date stencil as a set of dates.

        Parameters
        ----------
        reference_date : date
            Date to calculate offsets relative to.

        Returns
        -------
        set of pendulum.Date
            Set of dates represented by the stencil
        """
        date_pairs = self.as_date_pairs(reference_date=reference_date)
        dates = set().union(*[pendulum.period(pair[0], pair[1]) for pair in date_pairs])
        return dates

    def dates_are_available(
        self, reference_date: datetime.date, available_dates: Iterable[datetime.date]
    ) -> bool:
        """
        Check whether all dates represented by this date stencil for a
        particular reference date are included in a set of available dates.

        Parameters
        ----------
        reference_date : date
            Date to calculate offsets relative to.
        available_dates : iterable of date
            Set of available dates
        
        Returns
        -------
        bool
            True if all dates are available, False otherwise.
        
        Notes
        -----

        If the stencil is not valid for the given reference date (i.e. contains
        invalid date pairs), this function will return False.
        """
        try:
            set_of_dates = self.as_set_of_dates(reference_date=reference_date)
        except InvalidDateIntervalError:
            return False
        return set_of_dates.issubset(set(available_dates))
