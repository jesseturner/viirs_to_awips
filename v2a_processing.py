from v2a_utils import v2a_utils as v2a
from datetime import datetime
from pprint import PrettyPrinter

pp = PrettyPrinter(sort_dicts=False) # keeps in order of insertion

status = v2a.create_env()
status = v2a.create_logging(status)
#--- Maybe lock here

current_date = datetime.now().strftime('%Y-%m-%d')
status = v2a.time_window_selector(status, mode='day', target_date=current_date)

#--- change this so it gets full orbits
status = v2a.get_files_for_valid_orbits(status)

pp.pprint(v2a.summarize_lists_for_pprint(status))
