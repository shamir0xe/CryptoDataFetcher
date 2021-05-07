import datetime
import calendar

class TimeHelper:
    @staticmethod
    def current_utc_timestamp() -> int:
        return calendar.timegm(datetime.datetime.utcnow().utctimetuple())
