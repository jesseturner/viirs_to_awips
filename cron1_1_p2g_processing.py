from p2g_processing_utils import TimeWindowSelector, FileGrabber, Polar2Grid_Runner
from datetime import datetime

#tw1 = TimeWindowSelector(mode='current')
current_date = datetime.now().strftime('%Y-%m-%d')
tw1 = TimeWindowSelector(mode='day', target_date=current_date)

start_time, end_time = tw1.get_window()
print(f"Searching for data from {start_time.strftime('%Y-%m-%d, %H:%M:%S')} to {end_time.strftime('%H:%M:%S')}.")

selector = FileGrabber(start_time, end_time)
matching_files = selector.get_files_for_valid_orbits()
print(f"Grabbed {len(matching_files)} files.")

data_dirs = selector.copy_files_locally(matching_files)

p2g = Polar2Grid_Runner(data_dirs)

status = p2g.run_p2g()
print(f"Polar2Grid ran with status: {status}.")

p2g.name_and_move_files("/mnt/data1/jturner/viirs_awips/")