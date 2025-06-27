import argparse, subprocess
from datetime import datetime, timedelta


def main(raw_args=None):
    parser = argparse.ArgumentParser(
        description='Send an email if it\'s been a while since we\'ve gotten new data for NPP or J01 or J02')
    parser.add_argument('-t', '--time', type=int,
                        help='Specify a threshold (in hours) for how long is too long for new data to not be found')

    email_threshold_hours = 10  # This seems to be the longest "normal" gap we ever have (+/- an hour)
    args = parser.parse_args(raw_args)
    if args.time:
        email_threshold_hours = int(args.time)

    file_suffixes = ['NPP', 'J01', 'J02']

    for suffix in file_suffixes:
        with open('/mnt/viirs/WI-CONUS/Lists/WI-CONUS-' + suffix + '.txt') as recent_times_file: #--- this might not be correct
            last_time_split_raw = recent_times_file.readlines()
            last_time_split_filtered = [l for l in last_time_split_raw if len(l) > 1]  # Remove any lines that are just linebreaks
            last_time_split = last_time_split_filtered[-1].split('_')
            del last_time_split[3]  # Remove the day of year - unneeded

            # Split time into hours and minutes and cast everything to integers
            last_time_split.append(last_time_split[3][2:4])
            last_time_split[3] = last_time_split[3][0:2]
            last_time_split = [int(x) for x in last_time_split]

            last_pass_dt = datetime(last_time_split[0], last_time_split[1], last_time_split[2], last_time_split[3],
                                    last_time_split[4])
            if (datetime.now() - last_pass_dt) > timedelta(hours=email_threshold_hours):
                subprocess.Popen(['bash', 'cron3-1_send_files_outdated_email.sh', suffix, str(last_pass_dt),
                                  str(datetime.now().replace(microsecond=0))],
                                 cwd='/mnt/data1/jturner')
                
    print(f"=== Run at {datetime.now().isoformat()} ===")


if __name__ == "__main__":
    main()
