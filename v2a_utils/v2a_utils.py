import os, re
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
    bundle_ids = _get_valid_orbits(filenames, status['start_time'], status['end_time'])
    status['filenames'] = [f for f in filenames if any(f"b{bid}" in f for bid in bundle_ids)]
    
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