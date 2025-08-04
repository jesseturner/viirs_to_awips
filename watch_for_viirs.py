#--- Run with: 
#--- viirs_to_awips conda env
#--- nohup python -u ./watch_for_viirs.py >> /mnt/data1/jturner/watch_for_viirs.log 2>&1 &

#------ New version, making sure conda env is fine
#--- nohup bash -c "source ~/miniconda3/etc/profile.d/conda.sh && conda activate viirs_to_awips && python -u ./watch_for_viirs.py" >> /mnt/data1/jturner/watch_for_viirs.log 2>&1 &

#--- pkill -f watch_for_viirs.py

import os
import time
from datetime import datetime
#from watchdog.observers import Observer
from watchdog.observers.polling import PollingObserver as Observer
from watchdog.events import FileSystemEventHandler

BASE_PATHS = [
    "/mnt/jpssnas9/WI-CONUS/J01/SDR-IBand/{year}/{jday}",
    "/mnt/jpssnas9/WI-CONUS/J01/SDR-MBand/{year}/{jday}",
    "/mnt/jpssnas9/WI-CONUS/J02/SDR-IBand/{year}/{jday}",
    "/mnt/jpssnas9/WI-CONUS/J02/SDR-MBand/{year}/{jday}",
    "/mnt/jpssnas9/WI-CONUS/NPP/SDR-IBand/{year}/{jday}",
    "/mnt/jpssnas9/WI-CONUS/NPP/SDR-MBand/{year}/{jday}"
]

def get_watch_dirs():
    now = datetime.now()
    year = now.strftime("%Y")
    jday = now.strftime("%j")
    return [path.format(year=year, jday=jday) for path in BASE_PATHS]

class WatcherHandler(FileSystemEventHandler):
    def on_created(self, event):
        if not event.is_directory:
            now = datetime.now()
            timestamp = now.strftime("%Y-%m-%d %H:%M:%S")
            print(f"[{timestamp}] New file detected: {event.src_path}", flush=True)
            # You can run your processing script here if needed

def start_observers(paths):
    observers = []
    for path in paths:
        if os.path.exists(path):
            event_handler = WatcherHandler()
            observer = Observer()
            observer.schedule(event_handler, path=path, recursive=False)
            observer.start()
            observers.append((observer, path))
            print(f"Watching {path}...", flush=True)
        else:
            print(f"Path does not exist (skipping): {path}", flush=True)
    return observers

def stop_observers(observers):
    for observer, _ in observers:
        observer.stop()
    for observer, _ in observers:
        observer.join()

if __name__ == "__main__":
    current_paths = get_watch_dirs()
    observers = start_observers(current_paths)

    try:
        while True:
            time.sleep(30)
            new_paths = get_watch_dirs()
            if new_paths != current_paths:
                print("Date changed. Updating watch paths...", flush=True)
                stop_observers(observers)
                current_paths = new_paths
                observers = start_observers(current_paths)
    except KeyboardInterrupt:
        stop_observers(observers)