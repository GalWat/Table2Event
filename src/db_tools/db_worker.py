from texttable import Texttable
from datetime import datetime, date, time
from src.endpoint_api.amplitude.event import Event
from dataclasses import dataclass
from enum import Enum
from src.config import settings


from .db_joiner import DBJoiner


class DBManagementSystem(str, Enum):
    MYSQL = 'mysql'


class EndpointApi(str, Enum):
    AMPLITUDE = 'amplitude'


@dataclass
class EventTemplate:
    date_column: str
    event_properties_columns: list
    user_properties_columns: list


class DBWorker:
    def __init__(self):
        users_table_name = settings.users_table
        user_id_column_name = settings.user_id_column

        self.joiner = DBJoiner()

        self.user_id_column = f'{users_table_name}.{user_id_column_name}'
        self.main_table_name = users_table_name

        self.queries = list(self.joiner.join())

    def print_query(self, limit=10):
        for query in self.queries:
            rows = list(query.limit(limit))

            t = Texttable()
            t.set_max_width(0)
            t.add_rows([
                [col['expr'] for col in query.column_descriptions],
                *rows
            ])
            print(t.draw())

    def _generate_events(self):
        for query in self.queries:
            column_descriptions = query.column_descriptions

            columns_by_table = {}
            for col_desc in column_descriptions:
                col_expr = str(col_desc['expr'])
                table = col_expr.split('.')[0]  # 'employees.some' -> ['employees', 'some'] -> 'employees'
                try:
                    columns_by_table[table].append(col_expr)
                except KeyError:
                    columns_by_table[table] = [col_expr]

            possible_dates = {datetime, date, time}

            def filter_by_type(needed_types):
                def inner(column) -> bool:
                    try:
                        if column['type'].python_type in needed_types:
                            return True
                    except NotImplementedError:
                        pass

                    return False
                return inner

            all_id_columns = [str(col['expr']) for col in filter(filter_by_type([int]), column_descriptions)]
            all_date_columns = [str(col['expr']) for col in filter(filter_by_type(possible_dates), column_descriptions)]

            user_properties_columns = list(filter(
                lambda col: col not in [*all_id_columns, *all_date_columns],
                columns_by_table[self.main_table_name]
            ))

            event_templates = []

            for table, columns_in_table in columns_by_table.items():
                if table == self.main_table_name:
                    continue

                event_properties_columns = list(filter(
                    lambda col: col not in [*all_date_columns],
                    columns_in_table
                ))

                table_date_columns = list(filter(lambda col: col in all_date_columns, columns_in_table))
                for date_column in table_date_columns:
                    event_templates.append(EventTemplate(date_column, event_properties_columns, user_properties_columns))

            yield event_templates

    @property
    def extracted_events(self):
        events = list(self._generate_events())

        for query, query_events in zip(self.queries, events):
            columns = [str(col['expr']) for col in query.column_descriptions]

            for row in query[:1]:
                for event in query_events:
                    name = event.date_column
                    _date = row._data[columns.index(event.date_column)]
                    user_id = row._data[columns.index(self.user_id_column)]

                    event_properties = {col.split('.')[1]: row._data[columns.index(col)] for col in event.event_properties_columns}
                    user_properties = {col.split('.')[1]: row._data[columns.index(col)] for col in event.user_properties_columns}

                    yield Event(name, user_id, _date, event_properties, user_properties)
