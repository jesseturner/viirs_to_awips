from p2g_processing_utils import TimeWindowSelector, FileGrabber, Polar2Grid_Runner

# tw1 = TimeWindowSelector(mode='current')
#tw1 = TimeWindowSelector(mode='hour', target_date='2025-07-17', hour=6)
tw1 = TimeWindowSelector(mode='day', target_date='2025-07-17')
print(tw1.get_window())

start_time, end_time = tw1.get_window()

selector = FileGrabber(start_time, end_time)
matching_files = selector.get_files_for_valid_orbits()

data_dirs = selector.copy_files_locally(matching_files)

p2g = Polar2Grid_Runner(data_dirs)

status = p2g.run_p2g()
print(status)

p2g.name_and_move_files("/mnt/data1/jturner/viirs_awips/")