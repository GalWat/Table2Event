# TODO KM: refactor this
def get_join_column(first_table, second_table):
    foreign_keys = second_table.foreign_keys

    if foreign_keys:
        use_column = [(key.column, key.parent) for key in foreign_keys if
                      key.column.table is first_table].pop()
    else:
        self_foreign_keys = first_table.foreign_keys
        use_column = [(key.parent, key.column) for key in self_foreign_keys if
                      key.column.table is second_table].pop()

    return use_column

def get_table_relations(table):
    related = set()

    _nodes = getattr(table, '_nodes', [])
    related.update(_nodes)

    foreign = [key.column.table for key in table.foreign_keys]
    related.update(foreign)

    return related