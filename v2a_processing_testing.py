from v2a_utils import v2a_utils as v2a
from datetime import datetime
from pprint import PrettyPrinter

pp = PrettyPrinter(sort_dicts=False) # keeps in order of insertion

status = v2a.create_env()
status = v2a.create_logging(status)
#--- Maybe lock here

current_date = datetime.now().strftime('%Y-%m-%d')

status = v2a.time_window_selector(status, mode='current', duration_minutes=60) #--- option to add hour to current

# status = v2a.get_orbits_by_timestamp(status)
# pp.pprint(v2a.summarize_lists_for_pprint(status))

status = v2a.get_orbits_by_mod_time(status)
pp.pprint(v2a.summarize_lists_for_pprint(status))


# status = v2a.copy_files_locally(status)

# status = v2a.run_p2g(status)

# status = v2a.name_and_move_files(status)

# status = v2a.move_files_to_ldm(status)

# status = v2a.clean_up_to_ldm_recent(status)

#------------------

status = v2a.calc_total_run_time(status)

pp.pprint(v2a.summarize_lists_for_pprint(status))
