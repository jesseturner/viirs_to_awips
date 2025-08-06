import os
from datetime import datetime

def create_env():
    os.environ['LANG'] = 'en_US.UTF-8'
    # os.environ['PATH'] = 'PATH=/usr/bin:/bin:$PATH' <-- do this correctly, if necessary
    os.environ['ORGANIZATION'] = 'CIRA'

    return

def create_logging():
    date = datetime.now().strftime("%Y-%m-%d %H:%M")
    logging_dir = "/mnt/data1/jturner/v2a_logs"
    os.makedirs(logging_dir, exist_ok=True)
    print(f"=== Run at {date} ===")