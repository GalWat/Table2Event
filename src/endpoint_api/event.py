from dataclasses import dataclass
from datetime import datetime
import datetime as dt


@dataclass
class Event:
    name: str
    user_id: str
    _date: datetime
    event_properties: dict
    user_properties: dict

    @property
    def date(self):
        date_and_time = datetime.combine(self._date, dt.time())
        return round((date_and_time - datetime.fromtimestamp(0)).total_seconds()) * 1000
