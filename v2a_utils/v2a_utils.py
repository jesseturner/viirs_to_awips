import os, re, shutil, subprocess, glob, gzip
from datetime import datetime, timedelta

def create_env():
    os.environ['LANG'] = 'en_US.UTF-8'
    # os.environ['PATH'] = 'PATH=/usr/bin:/bin:$PATH' <-- do this correctly, if necessary
    os.environ['ORGANIZATION'] = 'CIRA'

    status = {}
    status['run_time'] = datetime.now()
    status['run_dir'] = "/mnt/data1/jturner"
    return status

def create_logging(status):
    date = datetime.now().strftime("%Y-%m-%d %H:%M")
    logging_dir = os.path.join(status['run_dir'], "v2a_logs")
    os.makedirs(logging_dir, exist_ok=True)
    print(f"=== Run at {date} ===", flush=True)

    return status

def time_window_selector(status, target_datetime_str=None, duration_minutes=15):
    """
    Args: 
        target_datetime_str (str): Date string in format "YYYY-MM-DD HH:MM".
        defaults to current datetime. 
    """
    target_datetime = datetime.now()
    
    if target_datetime_str:
        target_datetime = datetime.strptime(target_datetime_str, "%Y-%m-%d %H:%M")           
    
    status['start_time'] = target_datetime - timedelta(minutes=duration_minutes)
    status['end_time'] = target_datetime

    assert status['start_time'] < status['end_time'], f"Window start time ({status['start_time']}) is equal or later than end time ({status['end_time']})"
    
    print(f"Looking for data between {status['start_time'].strftime('%Y-%m-%d %H:%M UTC')} and {status['end_time'].strftime('%Y-%m-%d %H:%M UTC')}", flush=True)    
    return status
    
def get_orbits_by_timestamp(status):
    filenames = _get_all_filenames(status['start_time'])
    orbit_ids = _get_orbits_by_filename_timestamp(filenames, status['start_time'], status['end_time'])
    status['orbits'] = orbit_ids
    status['filenames'] = [f for f in filenames if any(f"b{orb}" in f for orb in orbit_ids)]
    
    return status

def get_orbits_by_mod_time(status):
    filenames = _get_all_filenames(status['start_time'])
    filenames_mod_time = _filter_filenames_by_mod_time(filenames, status)
    orbit_ids = _get_orbits_by_filename_all(filenames_mod_time, status['start_time'], status['end_time'])
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
            if os.listdir(band_dir):
                full_paths = [os.path.join(band_dir, fname) for fname in os.listdir(band_dir)]
                filenames.extend(full_paths)

    return filenames

def _filter_filenames_by_mod_time(filenames, status):
    filenames_mod_time = []
    for f in filenames:
        mod_time = datetime.fromtimestamp(os.path.getmtime(f))
        if status['start_time'] <= mod_time <= status['end_time']:
            filenames_mod_time.append(f)
    return filenames_mod_time

def _get_timestamp_and_orbit(filename: str):
    match = re.search(r"d(\d{8})_t(\d{6})\d?_.*_b(\d{5})_", filename)
    if not match:
        return None, None
    date_str, time_str, orbit = match.groups()
    timestamp = datetime.strptime(date_str + time_str, "%Y%m%d%H%M%S")
    
    return timestamp, orbit

def _get_orbits_by_filename_timestamp(filenames: list[str], start_time, end_time) -> set[str]:
    orbits = set()
    for fname in filenames:
        ts, orb = _get_timestamp_and_orbit(fname)
        if ts and start_time <= ts <= end_time:
            orbits.add(orb)
    
    return orbits

def _get_orbits_by_filename_all(filenames: list[str], start_time, end_time) -> set[str]:
    orbits = set()
    for fname in filenames:
        orb = re.search(r"_b(\d{5})_", fname)
        if orb:
            orbits.add(orb.group(1))
    
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
    print(f"Copying {len(status['filenames'])} files locally...", flush=True)
    current_dtstamp = datetime.now().strftime('d%Y%m%dt%H%M%S')
    created_dirs = set()
    sat_orbits = set()

    for f in status['filenames']:
        sat, band, orbit = _get_info_viirs_filename(f)
        sat_orbits.add(f'{sat}_{orbit}')

        processing_dir = os.path.join(status['run_dir'], f"1_viirs_for_p2g/{current_dtstamp}_{sat}_{band}_{orbit}")
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


def run_p2g(status):
    p2g_status = ""
    orbit_dir = os.path.join(status['run_dir'], "1_viirs_for_p2g/")
    for data_dir in os.listdir(orbit_dir):
        print(f"Running polar2grid for {data_dir}...", flush=True)
        
        raw_files_dir = os.path.join(orbit_dir, data_dir, "raw_files/")
        if re.search(r"MBand", data_dir): band = 'm'
        if re.search(r"IBand", data_dir): band = 'i'

        if os.listdir(raw_files_dir):  #--- checks if directory is not empty
            p2g_status = subprocess.call(
                ['bash', os.path.join(status['run_dir'], f'call_p2g_{band}.sh'), raw_files_dir],
                cwd=os.path.join(orbit_dir, data_dir)
            )
        else: 
            print(f"[ERROR] No files in : {raw_files_dir}", flush=True)
            continue

    assert p2g_status == 0 or p2g_status == "", f"Polar2grid error: {p2g_status}"

    return status

def name_and_move_files(status):
    print("Naming and moving files...", flush=True)
    output_dir = os.path.join(status['run_dir'], "2_viirs_awips_format/")
    orbit_dir = os.path.join(status['run_dir'], "1_viirs_for_p2g/")
    awips_timestamps = set()
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    for data_dir in os.listdir(orbit_dir): 
        data_path = os.path.join(orbit_dir, data_dir)   
        bands, sat_name = _get_bands_and_sat_name(data_dir)
        awips_timestamps = _get_awips_timestamps(data_path, awips_timestamps)

        for band in bands:
            search_path = os.path.join(orbit_dir, data_dir, f'SSEC_AII_{sat_name}_viirs_{band}*.nc')
            for filepath in glob.glob(search_path):
                _create_awips_file(filepath, band, output_dir)


        shutil.rmtree(os.path.join(orbit_dir, data_dir))

    status['awips_timestamps'] = awips_timestamps

    return status

def _get_bands_and_sat_name(data_dir):
    if re.search(r"MBand", data_dir): 
        bands = {'m08', 'm10', 'm11', 'm12', 'm13', 'm14', 'm15', 'm16'}
    if re.search(r"IBand", data_dir): 
        bands = {'i01', 'i02', 'i03', 'i04', 'i05'}

    if re.search(r"NPP", data_dir): sat_name = 'npp'
    if re.search(r"J01", data_dir): sat_name = 'noaa20'
    if re.search(r"J02", data_dir): sat_name = 'noaa21'

    return bands, sat_name

def _get_awips_timestamps(data_dir, awips_timestamps):
    files = glob.glob(os.path.join(data_dir, "*.nc"))
    pattern = re.compile(r'_(\d{4})\.nc$')
    
    for f in files:
        match = pattern.search(f)
        if match:
            awips_timestamps.add(match.group(1))

    return awips_timestamps

def _create_awips_file(filepath, band, output_dir):
    filename = os.path.basename(filepath)
    filename_pieces = filename.split('_')

    # polar2grid default is SSEC_AII_[raw_sat_name]_viirs_m##_LCC_Tttt_yyyymmdd_hhmm.nc
    # desired output is RAMMB_ppppp_bbb_yyyymmdd_hhmm_ttt.nc.gz
    new_filename = ('RAMMB_VIIRS' + '_' + band.upper() + '_' +
                    filename_pieces[7] + '_' + filename_pieces[8][:-3] + '_' +
                    filename_pieces[6][1:] + '.nc.gz')

    output_path = os.path.join(output_dir, new_filename)

    with open(filepath, 'rb') as f_in, gzip.open(output_path, 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)
    return

def calc_total_run_time(status):
    curr_time = datetime.now()
    diff = curr_time - status['run_time']
    minutes = round(diff.total_seconds() / 60, 2)
    status['run_time'] = f'{minutes} minutes'
    return status

def move_files_to_ldm(status):
    print("Moving files to LDM...", flush=True)
    storage_dir = os.path.join(status['run_dir'], "3_to_ldm_recent")
    os.makedirs(storage_dir, exist_ok=True)

    subprocess.call(['bash', os.path.join(status['run_dir'], 'move_files_to_ldm.sh')])
    return status

def clean_up_to_ldm_recent(status):
    print("Cleaning to_ldm_recent directory...", flush=True)
    storage_dir = os.path.join(status['run_dir'], "3_to_ldm_recent")
    
    #--- Delete any file older than 7 days
    for filename in os.listdir(storage_dir):
        filename_date_str = filename[16:24]
        file_date = datetime.strptime(filename_date_str, "%Y%m%d")
        is_file_outdated = file_date < datetime.today() - timedelta(days=7)
        if is_file_outdated:
            os.remove(os.path.join(storage_dir, filename))
    return status
        