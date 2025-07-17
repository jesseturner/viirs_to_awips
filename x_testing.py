from x_process import TimeWindowSelector, FileGrabber, Polar2Grid_Runner

tw1 = TimeWindowSelector(mode='current')
print(tw1.get_window())