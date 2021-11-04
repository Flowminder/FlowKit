from datetime import date, datetime


class ExposedDatetimeMixin:
    """
    Mixin that adds a getter + setter for `start_date` and `end_date` such that they are exposed internally to
    a class as datetime objects (`_start_dt` and `_end_dt` respectively), but externally as strings.
    """

    @property
    def start_date(self):
        return self._start_dt.strftime("%Y-%m-%d %H:%M:%S")

    @start_date.setter
    def start_date(self, value):
        if type(value) is str:
            self._start_dt = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        elif type(value) in [date, datetime]:
            self._start_dt = value
        else:
            raise TypeError("start_date must be datetime or yyyy-mm-dd hh:mm:ss")

    @property
    def end_date(self):
        return self._end_dt.strftime("%Y-%m-%d %H:%M:%S")

    @end_date.setter
    def end_date(self, value):
        if type(value) is str:
            self._end_dt = datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
        elif type(value) in [date, datetime]:
            self._end_dt = value
        else:
            raise TypeError("end_date must be datetime or yyyy-mm-dd hh:mm:ss")
