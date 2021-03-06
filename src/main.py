from sqlalchemy import Column, DDL, Integer, String, Table, event
from sqlalchemy.inspection import inspect

from src.db_tools import DBConnection, DBWorker

DEBUG = False

if DEBUG:
    from pprint import pprint

db_connection = DBConnection()

db = DBWorker(db_connection)
tables_to_watch = db.get_tables_to_watch()

log_table = Table(
    'log_table2event', db_connection.meta,
    Column('id', Integer, primary_key=True, autoincrement=True),
    Column('target_table', String(255), nullable=False),
    Column('target_id', Integer, nullable=False)
)

try:
    for table in tables_to_watch:
        primary_key = [key.name for key in inspect(table).primary_key][0]  # FIXME: Don't work with composite primary key

        trigger_expression = DDL(
            f'CREATE TRIGGER t2e_after_insert_{table.__name__} AFTER INSERT ON {table.__name__}\n'
            f'FOR EACH ROW INSERT INTO log_table2event (target_table, target_id) VALUE ("{table.__name__}", NEW.{primary_key});'
        )

        event.listen(log_table, "after_create", trigger_expression)

    db_connection.meta.create_all(db_connection.engine)

    needed_table = [tab for tab in tables_to_watch if tab.__name__ == "rental"][0]

    if DEBUG:
        pprint([item[0] for item in list(db_connection.execute("SHOW TRIGGERS;"))])

    input()
    print(list(db_connection.execute(log_table.select())))

finally:

    triggers = [item[0] for item in list(db_connection.execute("SHOW TRIGGERS;"))]
    for trigger in triggers:
        if trigger.startswith("t2e"):
            db_connection.execute(f"DROP TRIGGER {trigger}")
    log_table.drop(db_connection.engine)

# db.print_query()

# print(list(db.extracted_events))

# send_batch_events(db.extracted_events)

# db.generate

# with open('draft_events.yaml', 'w') as fout:
#     fout.write(yaml.dump(events))
