import os, re, shutil, subprocess
from datetime import datetime, timedelta

def create_env():
    os.environ['LANG'] = 'en_US.UTF-8'
    # os.environ['PATH'] = 'PATH=/usr/bin:/bin:$PATH' <-- do this correctly, if necessary
    os.environ['ORGANIZATION'] = 'CIRA'

    status = {}
    status['status'] = 'running'
    return status

def create_logging(status):
    date = datetime.now().strftime("%Y-%m-%d %H:%M")
    logging_dir = "/mnt/data1/jturner/v2a_logs"
    os.makedirs(logging_dir, exist_ok=True)
    print(f"=== Run at {date} ===")

    return status

def time_window_selector(status, mode='current', target_date=None, hour=None, duration_minutes=15):
    """
    Args:
        mode (str): 'current', 'hour', or 'day'
        target_date (str or datetime): e.g., '2025-07-17' or datetime object
        hour (int): for mode='hour', 0-23
        duration_minutes (int): only used for mode='current'
    """
    mode = mode.lower()
    if target_date: 
        target_date = datetime.strptime(target_date, "%Y-%m-%d")
    else: target_date = datetime.now()
    
    status['start_time'], status['end_time'] = _calculate_window(mode, target_date, hour, duration_minutes)

    print(f"Looking for data between {status['start_time'].strftime('%Y-%m-%d %H:%M UTC')} and {status['end_time'].strftime('%Y-%m-%d %H:%M UTC')}")    
    return status


def _calculate_window(mode, target_date, hour, duration_minutes):
    now = datetime.now()

    if mode == 'current':
        return now - timedelta(minutes=duration_minutes), now

    elif mode == 'hour':
        if hour is None:
            raise ValueError("Hour must be provided for mode='hour'")
        start = target_date.replace(hour=hour, minute=0, second=0, microsecond=0)
        end = start + timedelta(hours=1)
        return start, end

    elif mode == 'day':
        start = target_date.replace(hour=0, minute=0, second=0, microsecond=0)
        end = start + timedelta(days=1)
        return start, end

    else:
        raise ValueError("Mode must be 'current', 'hour', or 'day'")
    
def get_files_for_valid_orbits(status) -> list[str]:
    filenames = _get_all_filenames(status['start_time'])
    orbit_ids = _get_valid_orbits(filenames, status['start_time'], status['end_time'])
    status['orbits'] = orbit_ids
    status['filenames'] = [f for f in filenames if any(f"b{orb}" in f for orb in orbit_ids)]
    
    return status

def _get_all_filenames(start_time):
    bands_to_process = ['M', 'I']
    sats_to_process = ['NPP', 'J01', 'J02']

    filenames = []

    for sat in sats_to_process:
        for band in bands_to_process:
            band_dir =  f"/mnt/jpssnas9/WI-CONUS/{sat}/SDR-{band}Band/{start_time.year}/{start_time.timetuple().tm_yday}/"
            full_paths = [os.path.join(band_dir, fname) for fname in os.listdir(band_dir)]
            filenames.extend(full_paths)

    return filenames

def _get_timestamp_and_orbit(filename: str):
    match = re.search(r"d(\d{8})_t(\d{6})\d?_.*_b(\d{5})_", filename)
    if not match:
        return None, None
    date_str, time_str, orbit = match.groups()
    timestamp = datetime.strptime(date_str + time_str, "%Y%m%d%H%M%S")
    
    return timestamp, orbit

def _get_valid_orbits(filenames: list[str], start_time, end_time) -> set[str]:
    orbits = set()
    for fname in filenames:
        ts, orb = _get_timestamp_and_orbit(fname)
        if ts and start_time <= ts <= end_time:
            orbits.add(orb)
    
    return orbits

def summarize_lists_for_pprint(d, max_len=10):
    summarized = {}
    for key, value in d.items():
        if isinstance(value, list) and len(value) > max_len:
            summarized[key] = f"[{len(value)} items]"
        else:
            summarized[key] = value
    return summarized

def copy_files_locally(status):
    print(f"Copying {len(status['filenames'])} files locally...")
    current_dtstamp = datetime.now().strftime('d%Y%m%dt%H%M%S')
    created_dirs = set()
    sat_orbits = set()

    for f in status['filenames']:
        sat, band, orbit = _get_info_viirs_filename(f)
        sat_orbits.add(f'{sat}_{orbit}')

        processing_dir = f"{current_dtstamp}_{sat}_{band}_{orbit}/"
        raw_files_dir = os.path.join(processing_dir, 'raw_files')

        #--- Only create directories once
        if (sat, band, orbit) not in created_dirs:
            os.makedirs(raw_files_dir, exist_ok=True)
            created_dirs.add(processing_dir)
        
        shutil.copy(f, raw_files_dir)
    
    status['orbits'] = sat_orbits
    return status

def _get_info_viirs_filename(filename):
    filename_split = re.split(r'[_/]', filename)
    sat = filename_split[4]
    band = filename_split[5]
    orbit = filename_split[13]
    return sat, band, orbit


# def run_p2g(status):
#     p2g_status = "Did not run P2G."
#     for data_dir in list(self.data_dirs):
        
#         raw_files_dir = os.path.join("/mnt/data1/jturner/", data_dir, "raw_files/")
#         if re.search(r"MBand", data_dir): band = 'm'
#         if re.search(r"IBand", data_dir): band = 'i'

#         if os.listdir(raw_files_dir):  #--- checks if directory is not empty
#             p2g_status = subprocess.call(
#                 ['bash', os.path.join(os.getcwd(), f'call_p2g_{band}.sh'), raw_files_dir],
#                 cwd=data_dir
#             )
#         else: 
#             print(f"[ERROR] No files in : {raw_files_dir}")
#             continue

#     return p2g_status