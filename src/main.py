# from itertools import product

from src.config import settings
from src.db_tools import DBWorker

db = DBWorker(settings.users_table, settings.user_id_column)
db.print_query()

pass
# send_batch_events(db.extracted_events)

# db.generate

# with open('draft_events.yaml', 'w') as fout:
#     fout.write(yaml.dump(events))
