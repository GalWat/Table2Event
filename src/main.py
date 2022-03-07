# from itertools import product
# import itertools
import time
from pprint import pprint
from sqlalchemy import Table, DDL, Column, Integer, String, event
from src.db_tools import DBWorker
from src.db_tools import DBSession
from sqlalchemy.orm import Session
from sqlalchemy import insert
from sqlalchemy.inspection import inspect
from sqlalchemy.sql import func

session = DBSession()

db = DBWorker(session)
# pprint(list(db.extracted_events))
tables_to_watch = db.get_tables_to_watch()

log_table = Table(
        'log_table2event', session.meta,
        Column('id', Integer, primary_key=True, autoincrement=True),
        Column('target_table', String(255), nullable=False),
        Column('target_id', Integer, nullable=False)
    )

try:
    for table in tables_to_watch:
        primary_key = [key.name for key in inspect(table).primary_key][0]  # FIXME: Don't work with composite primaru key

        trigger_expression = DDL(
            f'CREATE TRIGGER t2e_after_insert_{table.__name__} AFTER INSERT ON {table.__name__}\n'
            f'FOR EACH ROW INSERT INTO log_table2event (target_table, target_id) VALUE ("{table.__name__}", NEW.{primary_key});'
        )

        event.listen(log_table, "after_create", trigger_expression)

    session.meta.create_all(session.engine)

    needed_table = [tab for tab in tables_to_watch if tab.__name__ == "rental"][0]

    input()

    with Session(session.engine) as _session:
        # pprint([item[0] for item in list(_session.execute("SHOW TRIGGERS;"))])
        # pprint(list(_session.execute("SHOW TRIGGERS;")))

        _session.execute(
            insert(needed_table).values(
                rental_date=func.now(),
                inventory_id=200,
                customer_id=2,
                return_date=func.now(),
                staff_id=2
            )
        )

        print(list(_session.execute(log_table.select())))

finally:
    with Session(session.engine) as _session:
        triggers = [item[0] for item in list(_session.execute("SHOW TRIGGERS;"))]
        for trigger in triggers:
            if trigger.startswith("t2e"):
                _session.execute(f"DROP TRIGGER {trigger}")
    log_table.drop(session.engine)


# db.print_query()

# print(list(db.extracted_events))

# send_batch_events(db.extracted_events)

# db.generate

# with open('draft_events.yaml', 'w') as fout:
#     fout.write(yaml.dump(events))
