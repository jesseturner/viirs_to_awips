import argparse, os, time
from datetime import datetime, timedelta

def main(raw_args=None):
    parser = argparse.ArgumentParser(description='Clean up files in to_ldm_recent so we only keep the last week')
    parser.add_argument('-t', '--timestamp', type=int,
                        help='Override the default cutoff (7 days ago) with a custom timestamp (YYYYMMDD)')

    args = parser.parse_args(raw_args)

    if args.timestamp:
        cutoff_dt = datetime.strptime(str(args.timestamp), "%Y%m%d")
    else:
        cutoff_dt = datetime.now() - timedelta(days=7)

    cutoff_epoch = time.mktime(cutoff_dt.timetuple())

    target_dir = os.getcwd()+'/to_ldm_recent'
    for filename in os.listdir(target_dir):
        if not filename.endswith('.nc.gz'):
            continue
        filepath = os.path.join(target_dir, filename)
        if os.path.isfile(filepath):
            file_mtime = os.path.getmtime(filepath)
            if file_mtime < cutoff_epoch:
                os.remove(filepath)

if __name__ == "__main__":
    main()