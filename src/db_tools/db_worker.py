from texttable import Texttable
from datetime import datetime, date, time
from src.endpoint_api.event import Event
from dataclasses import dataclass

POSSIBLE_DATES = {datetime, date, time}


@dataclass
class EventTemplate:
    date_column: str
    event_properties_columns: list


class DBWorker:
    def __init__(self, query, user_id_column='employees.emp_no', main_table_name='employees'):
        self.query = query
        self.user_id_column = user_id_column
        self.main_table_name = main_table_name

        self.raw_events = []
        self.user_properties_columns = []
        self._generate_events()


    def print_query(self, limit=10):
        rows = list(self.query.limit(limit))

        t = Texttable()
        t.set_max_width(0)
        t.add_rows([
            [col['expr'] for col in self.query.column_descriptions],
            *rows
        ])
        print(t.draw())

    def _generate_events(self):
        columns = self.query.column_descriptions
        group_columns = [(str(col_desc['expr']).split('.')[0], col_desc) for col_desc in columns]

        groups = {}

        for col in group_columns:
            try:
                groups[col[0]].append(col[1])
            except KeyError:
                groups[col[0]] = [col[1]]

        all_ids = list(filter(lambda x: x['type'].python_type == int, columns))
        all_dates = list(filter(lambda x: x['type'].python_type in POSSIBLE_DATES, columns))

        self.user_properties_columns = [str(col_desc['expr']) for col_desc in groups[self.main_table_name] if col_desc not in [*all_ids, *all_dates]]

        for group, values in groups.items():
            event_properties_columns = [str(col_desc['expr']) for col_desc in values if col_desc not in [*all_ids, *all_dates]]
            if group == self.main_table_name:
                event_properties_columns = []

            event_dates = [str(col_desc['expr']) for col_desc in values if col_desc in all_dates]
            for item in event_dates:
                self.raw_events.append(EventTemplate(item, event_properties_columns))

    @property
    def extracted_events(self):
        columns = [str(col['expr']) for col in self.query.column_descriptions]

        for row in self.query:
            for event in self.raw_events:
                name = event.date_column
                _date = row._data[columns.index(event.date_column)]
                user_id = row._data[columns.index(self.user_id_column)]

                event_properties = {col.split('.')[1]: row._data[columns.index(col)] for col in event.event_properties_columns}
                user_properties = {col.split('.')[1]: row._data[columns.index(col)] for col in self.user_properties_columns}

                yield Event(name, user_id, _date, event_properties, user_properties)
