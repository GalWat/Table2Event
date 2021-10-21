# from itertools import product

from src.config import settings
from src.db_tools import DBJoiner, DBWorker
from src.endpoint_api.amplitude_api import send_batch_events

db = DBWorker(DBJoiner(settings).query)
db.print_query()

pass
# send_batch_events(db.extracted_events)

# db.generate

# with open('draft_events.yaml', 'w') as fout:
#     fout.write(yaml.dump(events))
