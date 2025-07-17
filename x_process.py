from datetime import datetime, timedelta
import re, os, shutil, subprocess, sys, glob, gzip

class TimeWindowSelector:
    def __init__(self, mode='current', target_date=None, hour=None, duration_minutes=15):
        """
        Args:
            mode (str): 'current', 'hour', or 'day'
            target_date (str or datetime): e.g., '2025-07-17' or datetime object
            hour (int): for mode='hour', 0-23
            duration_minutes (int): only used for mode='current'
        """
        self.mode = mode.lower()
        if target_date: 
            self.target_date = datetime.strptime(target_date, "%Y-%m-%d")
        else: self.target_date = datetime.now()
        self.hour = hour
        self.duration_minutes = duration_minutes

        self.datetime_start, self.datetime_end = self._calculate_window()

    def _calculate_window(self):
        now = datetime.now()

        if self.mode == 'current':
            return now - timedelta(minutes=self.duration_minutes), now

        elif self.mode == 'hour':
            if self.hour is None:
                raise ValueError("Hour must be provided for mode='hour'")
            start = self.target_date.replace(hour=self.hour, minute=0, second=0, microsecond=0)
            end = start + timedelta(hours=1)
            return start, end

        elif self.mode == 'day':
            start = self.target_date.replace(hour=0, minute=0, second=0, microsecond=0)
            end = start + timedelta(days=1)
            return start, end

        else:
            raise ValueError("Mode must be 'current', 'hour', or 'day'")

    def get_window(self):
        return self.datetime_start, self.datetime_end
    

class FileGrabber:
    def __init__(self, start_time: datetime, end_time: datetime):
        self.start_time = start_time
        self.end_time = end_time

    def _get_all_filenames(self):
        bands_to_process = ['M', 'I']
        sats_to_process = ['NPP', 'J01', 'J02']

        filenames = []

        for sat in sats_to_process:
            for band in bands_to_process:
                band_dir =  f"/mnt/jpssnas9/WI-CONUS/{sat}/SDR-{band}Band/{self.start_time.year}/{self.start_time.timetuple().tm_yday}/"
                full_paths = [os.path.join(band_dir, fname) for fname in os.listdir(band_dir)]
                filenames.extend(full_paths)

        return filenames

    def _get_timestamp_and_orbit(self, filename: str):
        match = re.search(r"d(\d{8})_t(\d{6})\d?_.*_b(\d{5})_", filename)
        if not match:
            return None, None
        date_str, time_str, orbit = match.groups()
        timestamp = datetime.strptime(date_str + time_str, "%Y%m%d%H%M%S")
        return timestamp, orbit

    def _get_valid_orbits(self, filenames: list[str]) -> set[str]:
        orbits = set()
        for fname in filenames:
            ts, orb = self._get_timestamp_and_orbit(fname)
            if ts and self.start_time <= ts <= self.end_time:
                orbits.add(orb)
        return orbits

    def get_files_for_valid_orbits(self) -> list[str]:
        filenames = self._get_all_filenames()
        bundle_ids = self._get_valid_orbits(filenames)
        return [f for f in filenames if any(f"b{bid}" in f for bid in bundle_ids)]

    def copy_files_locally(self, filenames: list[str]):
        current_dtstamp = datetime.now().strftime('d%Y%m%dt%H%M%S')
        created_dirs = set()

        for f in filenames:
            filename_split = re.split(r'[_/]', f)
            sat = filename_split[4]
            band = filename_split[5]
            orbit = filename_split[13]

            processing_dir = f"{current_dtstamp}_{sat}_{band}_{orbit}/"
            raw_files_dir = os.path.join(processing_dir, 'raw_files')

            #--- Only create directories once
            if (sat, band, orbit) not in created_dirs:
                os.makedirs(raw_files_dir, exist_ok=True)
                created_dirs.add(processing_dir)
            
            shutil.copy(f, raw_files_dir)
        
        return created_dirs

class Polar2Grid_Runner:
    def __init__(self, data_dirs: set):
        for path in data_dirs:
            if not os.path.exists(path):
                print(f"[ERROR] Path does not exist: {path}")
                sys.exit(1)
        self.data_dirs = data_dirs

    def run_p2g(self):
        for data_dir in list(self.data_dirs):
            
            raw_files_dir = os.path.join("/mnt/data1/jturner/", data_dir, "raw_files/")
            if re.search(r"MBand", data_dir): band = 'm'
            if re.search(r"IBand", data_dir): band = 'i'

            if os.listdir(raw_files_dir):  #--- checks if directory is not empty
                p2g_status = subprocess.call(
                    ['bash', os.path.join(os.getcwd(), f'call_p2g_{band}.sh'), raw_files_dir],
                    cwd=data_dir
                )
            else: 
                print(f"[ERROR] No files in : {raw_files_dir}")
                continue

        return p2g_status
    
    def name_and_move_files(self, output_dir: str):
        if not os.path.exists(output_dir):
            os.makedirs(output_dir)

        for data_dir in list(self.data_dirs):
        
            if re.search(r"MBand", data_dir): 
                bands = {'m08', 'm10', 'm11', 'm12', 'm13', 'm14', 'm15', 'm16'}
            if re.search(r"IBand", data_dir): 
                bands = {'i01', 'i02', 'i03', 'i04', 'i05'}

            if re.search(r"NPP", data_dir): sat_name = 'npp'
            if re.search(r"J01", data_dir): sat_name = 'noaa20'
            if re.search(r"J02", data_dir): sat_name = 'noaa21'

            for band in bands:
                for filepath in glob.glob(
                        data_dir + 'SSEC_AII_' + sat_name + '_viirs_' + band + '*.nc'):
                    filename = os.path.basename(filepath)
                    filename_pieces = filename.split('_')

                    # polar2grid default is SSEC_AII_[raw_sat_name]_viirs_m##_LCC_Tttt_yyyymmdd_hhmm.nc
                    # desired output is RAMMB_ppppp_bbb_yyyymmdd_hhmm_ttt.nc.gz
                    new_filename = ('RAMMB_VIIRS' + '_' + band.upper() + '_' +
                                    filename_pieces[7] + '_' + filename_pieces[8][:-3] + '_' +
                                    filename_pieces[6][1:] + '.nc.gz')

                    with open(filepath, 'rb') as f_in, gzip.open(output_dir + new_filename, 'wb') as f_out:
                        f_out.writelines(f_in)

            if os.listdir(output_dir):
                shutil.rmtree(data_dir)
            




start_time = datetime(2025, 7, 17, 6, 0, 0)
end_time = datetime(2025, 7, 17, 6, 30, 0)

selector = FileGrabber(start_time, end_time)
matching_files = selector.get_files_for_valid_orbits()

data_dirs = selector.copy_files_locally(matching_files)

p2g = Polar2Grid_Runner(data_dirs)

status = p2g.run_p2g()
print(status)

p2g.name_and_move_files("/mnt/data1/jturner/viirs_awips_x/")