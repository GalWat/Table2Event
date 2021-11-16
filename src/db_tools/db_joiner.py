from sqlalchemy import MetaData
from sqlalchemy.orm import Session

from .db_session import DBSession
from .common_functions import get_join_column, get_table_relations


class DBJoiner:
    def __init__(self, users_table_name):
        self.engine = DBSession.create_engine()

        self.meta = MetaData()
        self.meta.reflect(bind=self.engine)

        # TODO KM: set meta for testing
        self.tables = self.meta.tables
        self.add_backrefs()

        self.main_table = self.tables[users_table_name]

    def add_backrefs(self):
        for table in self.tables.values():
            for key in table.foreign_keys:
                try:
                    key.column.table._nodes.append(table)
                except AttributeError:
                    key.column.table._nodes = [table]

    def join_all(self):
        joined = [self.main_table]
        join_pipe = []

        for table in joined:
            for relation in get_table_relations(table):
                if relation in joined:
                    continue

                joined.append(relation)
                join_pipe.append((relation, get_join_column(table, relation)))

        columns = []

        temp_columns = [[y for y in x.columns if not y.foreign_keys] for x in joined]
        for col_list in temp_columns:
            columns.extend(col_list)

        return self.proceed_join_pipe([join_pipe], [columns])

    def join_by_pairs(self):
        join_pipe = []
        columns = []

        possible_pairs = get_table_relations(self.main_table)

        for table in possible_pairs:
            join_pipe.append([(table, get_join_column(self.main_table, table))])
            single_columns = []
            single_columns.extend(self.main_table.columns)
            single_columns.extend(table.columns)
            columns.append(single_columns)

        return self.proceed_join_pipe(join_pipe, columns)

    def proceed_join_pipe(self, join_pipe, columns):
        with Session(self.engine) as session:
            for single_join_pipe, single_columns in zip(join_pipe, columns):
                query = session.query(*single_columns)

                for relation, use_column in single_join_pipe:
                    query = query.join(relation, use_column[0] == use_column[1])

                yield query
