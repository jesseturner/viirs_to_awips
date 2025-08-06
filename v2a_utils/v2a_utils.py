import os
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
    
    datetime_start, datetime_end = _calculate_window(mode, target_date, hour, duration_minutes)

    print(f"Looking for data between {datetime_start.strftime('%Y-%m-%d %H:%M UTC')} and {datetime_end.strftime('%Y-%m-%d %H:%M UTC')}")
    status['window'] = datetime_start, datetime_end
    
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