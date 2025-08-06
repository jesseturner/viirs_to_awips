from v2a_utils import v2a_utils as v2a
from datetime import datetime
from pprint import pprint

status = v2a.create_env()
status = v2a.create_logging(status)
pprint(status)
# --- Maybe lock here

current_date = datetime.now().strftime('%Y-%m-%d')
status = v2a.time_window_selector(status, mode='current', target_date=current_date)
pprint(status)