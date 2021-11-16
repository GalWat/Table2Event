from texttable import Texttable
from datetime import datetime, date, time
from src.endpoint_api.amplitude.event import Event
from dataclasses import dataclass
from enum import Enum


from .db_joiner import DBJoiner


POSSIBLE_DATES = {datetime, date, time}


class DBManagementSystem(str, Enum):
    MYSQL = 'mysql'


class JoinModes(str, Enum):
    ALL = 'all'
    PAIRS = 'pairs'


class EndpointApi(str, Enum):
    AMPLITUDE = 'amplitude'


@dataclass
class EventTemplate:
    date_column: str
    event_properties_columns: list


class DBWorker:
    def __init__(self, users_table_name, user_id_column_name):
        self.joiner = DBJoiner(users_table_name)
        # FIXME: delete query
        self.querys = list(self.joiner.join_by_pairs())
        # self.query = self.querys[0]

        self.user_id_column = f'{users_table_name}.{user_id_column_name}'
        self.main_table_name = users_table_name

        self.raw_events = []
        self.user_properties_columns = []

    def print_query(self, limit=10):
        for query in self.querys:
            rows = list(query.limit(limit))

            t = Texttable()
            t.set_max_width(0)
            t.add_rows([
                [col['expr'] for col in query.column_descriptions],
                *rows
            ])
            print(t.draw())

    def _generate_events(self):
        for query in self.querys:
            columns = query.column_descriptions
            group_columns = [(str(col_desc['expr']).split('.')[0], col_desc) for col_desc in columns]

            groups = {}

            for col in group_columns:
                try:
                    groups[col[0]].append(col[1])
                except KeyError:
                    groups[col[0]] = [col[1]]

            all_ids = list(filter(lambda x: x['type'].python_type == int, columns))
            all_dates = list(filter(lambda x: x['type'].python_type in POSSIBLE_DATES, columns))

            user_properties_columns = [str(col_desc['expr']) for col_desc in groups[self.main_table_name] if col_desc not in [*all_ids, *all_dates]]
            raw_events = []

            for group, values in groups.items():
                event_properties_columns = [str(col_desc['expr']) for col_desc in values if col_desc not in [*all_ids, *all_dates]]
                if group == self.main_table_name:
                    # event_properties_columns = []
                    continue

                event_dates = [str(col_desc['expr']) for col_desc in values if col_desc in all_dates]
                for item in event_dates:
                    raw_events.append(EventTemplate(item, event_properties_columns))

            yield user_properties_columns, raw_events

    @property
    def extracted_events(self):
        events = list(self._generate_events())

        for query, cur_events in zip(self.querys, events):
            columns = [str(col['expr']) for col in query.column_descriptions]

            for row in query[:1]:
                for event in cur_events[1]:
                    name = event.date_column
                    _date = row._data[columns.index(event.date_column)]
                    user_id = row._data[columns.index(self.user_id_column)]

                    event_properties = {col.split('.')[1]: row._data[columns.index(col)] for col in event.event_properties_columns}
                    user_properties = {col.split('.')[1]: row._data[columns.index(col)] for col in cur_events[0]}

                    yield Event(name, user_id, _date, event_properties, user_properties)
