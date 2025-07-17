from datetime import datetime
import re, os, shutil


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
                created_dirs.add((sat, band, orbit))
            
            shutil.copy(f, raw_files_dir)

    

# start_time = datetime(2025, 7, 17, 5, 0, 0)
# end_time = datetime(2025, 7, 17, 5, 30, 0)

# selector = FileGrabber(start_time, end_time)
# matching_files = selector.get_files_for_valid_orbits()

# selector.copy_files_locally(matching_files)

