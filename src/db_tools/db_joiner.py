from sqlalchemy import MetaData
from sqlalchemy.orm import Session

from .db_session import DBSession


class DBJoiner:
    def __init__(self, users_table_name):
        self.engine = DBSession.create_engine()

        self.meta = MetaData()
        self.meta.reflect(bind=self.engine)

        # TODO KM: set meta for testing
        self.tables = self.meta.tables
        self.add_backrefs()

        self.main_table = self.tables[users_table_name]

        self.join_order = []
        self.columns = []

        self.create_join_order_and_parse_columns()

    def add_backrefs(self):
        for table in self.tables.values():
            for key in table.foreign_keys:
                try:
                    key.column.table._nodes.append(table)
                except AttributeError:
                    key.column.table._nodes = [table]

    @staticmethod
    def get_table_relations(table):
        related = set()

        _nodes = getattr(table, '_nodes', [])
        related.update(_nodes)

        foreign = [key.column.table for key in table.foreign_keys]
        related.update(foreign)

        return related

    def create_join_order_and_parse_columns(self):
        self.join_order = [self.main_table]

        for table in self.join_order:
            for relat in self.get_table_relations(table):
                if relat not in self.join_order:
                    self.join_order.append(relat)

        temp_columns = [[y for y in x.columns if not y.foreign_keys] for x in self.join_order]
        for col_list in temp_columns:
            self.columns.extend(col_list)

    def join_all(self):
        self.create_join_order_and_parse_columns()

        with Session(self.engine) as session:
            query = session.query(*self.columns)

            joined = [self.main_table]

            for table in self.join_order:
                for relation in self.get_table_relations(table):
                    if relation in joined:
                        continue

                    foreign_keys = relation.foreign_keys

                    if foreign_keys:
                        use_column = [(key.column, key.parent) for key in foreign_keys if
                                      key.column.table is table].pop()
                    else:
                        self_foreign_keys = table.foreign_keys
                        use_column = [(key.parent, key.column) for key in self_foreign_keys if
                                      key.column.table is relation].pop()

                    joined.append(relation)
                    query = query.join(relation, use_column[0] == use_column[1])

            return query

    def join_by_pairs(self):
        pass
