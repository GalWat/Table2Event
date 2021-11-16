# from itertools import product
# import itertools
from pprint import pprint

from src.config import settings
from src.db_tools import DBWorker

db = DBWorker(settings.users_table, settings.user_id_column)
# db.print_query()

# print(list(db.extracted_events))

pprint(list(db.extracted_events))

pass
# send_batch_events(db.extracted_events)

# db.generate

# with open('draft_events.yaml', 'w') as fout:
#     fout.write(yaml.dump(events))
