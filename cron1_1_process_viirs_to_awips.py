#------ built to run on polarbear3 with python3

from datetime import datetime, timezone, timedelta
import argparse, glob, gzip, logging, os, shutil, subprocess, re, sys
from dataclasses import dataclass
from pprint import pprint

#=====================================================

@dataclass
class OrbitFiles:
    sat: str = None
    orbit: str = None
    filepaths: list[str] = None
    raw_files_dir: str = None
    processing_dir: str = None
    missing_p2g_tags: list[str] = None

@dataclass #--- Config settings
class BaseState:
    base_dir: str = None
    sats_to_process: list[str] = None
    bands_to_process: list[str] = None
    current_dt: datetime = None
    file_dt: datetime = None
    log_prefix: str = None
    dtstamp_dir: str = None
    final_dir: str = None
    band_params: dict[str, str] = None
    raw_sat_names: dict[str, str] = None


#=====================================================

def main(raw_args=None):

    base = BaseState()
    setUpVariables(base)
    startLogging(base)
    parseArguments(raw_args, base)
    createTempAndOutputDir(base)
    setSatellitesAndBands(base)
    #pprint(base)
    orbits_to_process = getOrbits(base)

    for band in base.bands_to_process:
        for orbit_file in orbits_to_process:

            gettingFilesFromOrbit(base, band, orbit_file)
            grabbingViirsFiles(base, band, orbit_file)
            runningPolar2Grid(base, band, orbit_file)
            checkForMissingData(base, band, orbit_file)
            nameAndFillFiles(base, band, orbit_file)
            removeTempFiles(orbit_file)

            #pprint(orbit_file)

    finishAndClean(base)
    
#=====================================================

def setUpVariables(base: BaseState):

    base.base_dir = os.getcwd()
    base.bands_to_process = ['m', 'i']
    base.sats_to_process = ['NPP', 'J01', 'J02']
    base.current_dt = datetime.now(timezone.utc) # datetime(2025, 7, 11, 6, 33, 33)
    base.file_dt = datetime.now(timezone.utc) # datetime(2025, 7, 11, 6, 33, 33)

    return

#-----------------------------------------------------

def startLogging(base: BaseState): 

    logging_dir = base.base_dir + '/logs/'
    if not os.path.exists(logging_dir):
        os.makedirs(logging_dir)
    logging.basicConfig(filename=logging_dir + base.current_dt.strftime('%Y%m%d') + '.log', level=logging.INFO)
    base.log_prefix = f"{base.current_dt.strftime('%Y-%m-%d %H:%M:%S')} Z - "

    return

#-----------------------------------------------------

def parseArguments(raw_args, base: BaseState):

    #--- arguments are in the form of a list of strings, i.e. ['-d', '20250611']
    parser = argparse.ArgumentParser(description='Process incoming data from /mnt/jpssnas9/WI-CONUS/NPP for AWIPS ingestion')
    parser.add_argument('-f', '--freq-band', type=str,
                        help='Only process the files for the indicated band groupings (valid inputs are "i" and "m"')
    parser.add_argument('-d', '--file-date', type=str,
                        help='Run for the full 24 hours of a specific date (YYYYMMDD format) or for a specific hour (YYYYMMDDhh format)')
    
    args = parser.parse_args(raw_args)

    #--- No arguments
    if len(sys.argv) == 1:
        start_time = base.file_dt - timedelta(minutes=12)
        logging.info(f"{base.log_prefix} Looking for data from {start_time.strftime('%Y-%m-%d %H:%M')} to {base.file_dt.strftime('%H:%M')} UTC")

    #--- Specified band
    if args.freq_band:
        if args.freq_band not in base.bands_to_process:
            logging.error('Invalid single-band input; valid options are "i" and "m"')
            exit(1)
        base.bands_to_process = [args.freq_band]

    #--- Specified date
    if args.file_date:
        #--- Depending on if hour is specified or not
        if len(args.file_date) == 10:  #--- format: YYYYMMDDhh
            base.file_dt = datetime.strptime(args.file_date, "%Y%m%d%H")
            base.file_dt = base.file_dt.replace(tzinfo=timezone.utc)
            logging.info(f"{base.log_prefix} Looking for data from {base.file_dt.strftime('%Y-%m-%d %H')}:00 UTC")
        elif len(args.file_date) == 8:  #--- format: YYYYMMDD
            base.file_dt = datetime.strptime(args.file_date, "%Y%m%d")
            base.file_dt = base.file_dt.replace(tzinfo=timezone.utc)
            logging.info(f"{base.log_prefix} Looking for all data from {base.file_dt.strftime('%Y-%m-%d')}")
        else:
            raise ValueError("file_date must be in YYYYMMDD or YYYYMMDDhh format")
        
    
    return

#-----------------------------------------------------

def getOrbits(base: BaseState):

    orbits_to_process = []
    
    for sat in base.sats_to_process:    #--- NPP, J01, J02
        for band in base.bands_to_process:   #--- m, i
            
            #--- set band parameters
            band_dir = base.band_params[band]['band_dir'].replace('_replacewithsat_', sat)

            #--- create search term for files
            #------ this needs improvement, in case 00 hour is selected in argument
            if base.file_dt.hour == 0 and base.file_dt.microsecond == 0: #--- if full day (YYYYMMDD) is provided in argument
                file_date_start = datetime(base.file_dt.year, base.file_dt.month, base.file_dt.day, 0, 0, 0)
                file_date_end = file_date_start + timedelta(days=1)

            elif base.file_dt.hour != 0 and base.file_dt.microsecond == 0: #--- if hour is (YYYYMMDDhh) is provided in argument
                file_date_start = base.file_dt
                file_date_end = base.file_dt + timedelta(hours=1)
            
            elif base.file_dt.microsecond != 0:
                file_date_start = base.file_dt - timedelta(minutes=12)
                file_date_end = base.file_dt
                
            else:
                logging.error(f'Improper time selection: {base.file_dt.strftime("%Y-%m-%d %H:%M")}')
                exit(1)

            #--- Avoiding offset datetime error
            file_date_start = file_date_start.replace(tzinfo=timezone.utc)
            file_date_end = file_date_end.replace(tzinfo=timezone.utc)
            
            #--- get files that match time range
            matching_files = []
            for f in os.listdir(band_dir):
                match = re.search(r'd(\d{8})_t(\d{2})(\d{2})(\d{2})\d', f)
                if match:
                    date_str, hour, minute, second = match.groups()
                    dt = datetime.strptime(f"{date_str}{hour}{minute}{second}", "%Y%m%d%H%M%S")
                    dt = dt.replace(tzinfo=timezone.utc)

                    if file_date_start <= dt <= file_date_end:
                        matching_files.append(f)

            if not matching_files:
                continue

            #--- create orbits list from matching files
            for filename in matching_files:  
                orbit = filename.split('_')[5]
                if orbit not in [of.orbit for of in orbits_to_process]:
                    orbit_file = OrbitFiles(sat=sat, orbit=orbit)
                    orbits_to_process.append(orbit_file)
    
    return orbits_to_process

#-----------------------------------------------------

def createTempAndOutputDir(base: BaseState):
    #--- creating a YYYYMMDD_hhmmss dir for an isolated workspace
    base.dtstamp_dir = f"{base.base_dir}/{base.current_dt.strftime('%Y%m%d%H%M%S')}/"
    if not os.path.exists(base.dtstamp_dir):
        os.makedirs(base.dtstamp_dir)

    #--- creating the output directories
    base.final_dir = base.base_dir + '/viirs_awips/'
    if not os.path.exists(base.final_dir):
        os.makedirs(base.final_dir)
    
    return

#-----------------------------------------------------

def setSatellitesAndBands(base: BaseState):
    #--- list of products processed
    #------ (previous note) when you add/remove products, you need to update the ldm injection script on the LDM server (cira-ldm1)
    m_ldm_file_tags = {'m08': 'M08', 'm10': 'M10', 'm11': 'M11', 'm12': 'M12', 'm13': 'M13', 'm14': 'M14', 'm15': 'M15', 'm16': 'M16'}
    i_ldm_file_tags = {'i01': 'I01', 'i02': 'I02', 'i03': 'I03', 'i04': 'I04', 'i05': 'I05'}

    #--- set up dictionaries for each band
    base.band_params = {
        'm': {
            'band_dir': f"/mnt/jpssnas9/WI-CONUS/_replacewithsat_/SDR-MBand/{base.file_dt.year}/{base.file_dt.timetuple().tm_yday}/",
            'prod_prefixes': ['GMTCO'] + ['SV' + tag for tag in list(m_ldm_file_tags.values())],
            'ldm_file_tags': m_ldm_file_tags,
            'output_prod_name': 'VIIRS'
        },
        'i': {
            'band_dir': f"/mnt/jpssnas9/WI-CONUS/_replacewithsat_/SDR-IBand/{base.file_dt.year}/{base.file_dt.timetuple().tm_yday}/",
            'prod_prefixes': ['GITCO'] + ['SV' + tag for tag in list(i_ldm_file_tags.values())],
            'ldm_file_tags': i_ldm_file_tags,
            'output_prod_name': 'VIIRS'
        }
    }

    base.raw_sat_names = {'NPP': 'npp', 'J01': 'noaa20' , 'J02': 'noaa21'}

    return

#-----------------------------------------------------

def gettingFilesFromOrbit(base, band, orbit_file):

    orbit_file.filepaths = [] #--- Initialize

    prod_prefixes = base.band_params[band]['prod_prefixes']

    #--- create list of recent orbit files
    band_dir = base.band_params[band]['band_dir'].replace('_replacewithsat_', orbit_file.sat)
    orbit_files_recent = [f for f in os.listdir(band_dir) if
                        f[0:5] in prod_prefixes and '_' + orbit_file.orbit + '_' in f]
    
    #--- count number of orbit files, warning if one of the bands has a different number
    #------ I bet there is a simpler way of doing this
    num_files = len([f for f in orbit_files_recent if f[0:5] == prod_prefixes[0]])
    orbit_not_ready = False

    for prefix in prod_prefixes:
        if num_files != len([f for f in orbit_files_recent if f[0:5] == prefix]):
            orbit_not_ready = True
            break
        if orbit_not_ready:
            logging.info(f"{orbit_file.sat} orbit {orbit_file.orbit} bands are not fully filled in yet, not processing")
            continue
        
        #--- logging the files used
        orbit_file.filepaths.extend(glob.glob(os.path.join(band_dir, prefix + '*_' + orbit_file.orbit + '_*')))
        if not orbit_file.filepaths:
            continue
        else: 
            orbit_file.filepaths.sort()  #--- sort to follow time order
            first_file = os.path.basename(orbit_file.filepaths[0])
            match = re.search(r'd(\d{8})_t(\d{2})(\d{2})', first_file)
            if match:
                date_str, hour, minute = match.groups()
                dt = datetime.strptime(f"{date_str}{hour}{minute}", "%Y%m%d%H%M")
                datetime_str = dt.strftime("%Y-%m-%d %H:%M UTC")
            else:
                datetime_str = "Unknown datetime"
                
    if orbit_file.filepaths:
        logging.info(f"Processing {len(orbit_file.filepaths)} VIIRS files for {orbit_file.sat} orbit {orbit_file.orbit} {band}-band at {datetime_str}")  
    
    return
    
#-----------------------------------------------------

def grabbingViirsFiles(base, band, orbit_file):

    orbit_file.processing_dir = base.dtstamp_dir + orbit_file.sat + '_' + band + '_' + orbit_file.orbit + '/'
    os.makedirs(orbit_file.processing_dir)
    orbit_file.raw_files_dir = orbit_file.processing_dir + 'raw_files/'
    os.makedirs(orbit_file.raw_files_dir)

    for filepath in orbit_file.filepaths:
        shutil.copy(filepath, orbit_file.raw_files_dir) #--- copying the file
            
    return
    
#-----------------------------------------------------

def runningPolar2Grid(base, band, orbit_file):
    #--- running the polar2grid package
    #------ this is the very core of the processing
        
    print(base.log_prefix + 'Running p2g for ' + orbit_file.sat + ' ' + band + ' band ')
    if os.listdir(orbit_file.raw_files_dir):  #--- checks if directory is not empty
        p2g_status = subprocess.call(
            ['bash', os.path.join(base.base_dir, f'call_p2g_{band}.sh'), orbit_file.raw_files_dir],
            cwd=orbit_file.processing_dir
        )

        print(f"{base.log_prefix} Finished running p2g for {orbit_file.sat} (orbit {orbit_file.orbit}) {band} band with exit status {p2g_status}")
                
    return
    
#-----------------------------------------------------

def checkForMissingData(base, band, orbit_file):
    
    raw_sat_name = base.raw_sat_names[orbit_file.sat]

    #--- check if channels are missing
    #------ there is definitely a simpler way of doing this
    
    ldm_file_tags = base.band_params[band]['ldm_file_tags']
    p2g_file_tags = list(ldm_file_tags.keys())

    orbit_file.missing_p2g_tags = []
    for p2g_tag in p2g_file_tags:
        if len(glob.glob(orbit_file.processing_dir + 'SSEC_AII_' + raw_sat_name + '_viirs_' + p2g_tag + '*.nc')) == 0:
            orbit_file.missing_p2g_tags.append(p2g_tag)
    #--- all files are missing tags, likely due to <10% grid coverage
    if len(orbit_file.missing_p2g_tags) == len(p2g_file_tags):
        logging.info(f"P2G returned no files for {orbit_file.sat} orbit {orbit_file.orbit} {band}-band at {base.file_dt.strftime('%Y-%m-%d %H:%M UTC')}")

    #--- some files are missing tags, likely due to lack of sun
    elif len(orbit_file.missing_p2g_tags) > 0:
        logging.info(f"P2G rejected {orbit_file.missing_p2g_tags} for {orbit_file.sat} orbit {orbit_file.orbit} {band}-band at {base.file_dt.strftime('%Y-%m-%d %H:%M UTC')}")
    
    return

#-----------------------------------------------------

def nameAndFillFiles(base, band, orbit_file):
    #--- for each file type, names properly and fills with gzip-compressed data
    
    raw_sat_name = base.raw_sat_names[orbit_file.sat]

    ldm_file_tags = base.band_params[band]['ldm_file_tags']
    p2g_file_tags = list(ldm_file_tags.keys())
    output_prod_name = base.band_params[band]['output_prod_name']

    file_count = 0
    for p2g_tag in p2g_file_tags:
        for filepath in glob.glob(
                orbit_file.processing_dir + 'SSEC_AII_' + raw_sat_name + '_viirs_' + p2g_tag + '*.nc'):
            filename = os.path.basename(filepath)
            filename_pieces = filename.split('_')

            # polar2grid default is SSEC_AII_[raw_sat_name]_viirs_m##_LCC_Tttt_yyyymmdd_hhmm.nc
            # desired output is RAMMB_ppppp_bbb_yyyymmdd_hhmm_ttt.nc.gz
            # (ppppp = VIIRS, others later?), (bbb = band, e.g. M08, I03)
            new_filename = ('RAMMB_' + output_prod_name + '_' + ldm_file_tags[p2g_tag] + '_' +
                            filename_pieces[7] + '_' + filename_pieces[8][:-3] + '_' +
                            filename_pieces[6][1:] + '.nc.gz')

            if p2g_tag not in orbit_file.missing_p2g_tags:
                with open(filepath, 'rb') as f_in, gzip.open(base.final_dir + new_filename, 'wb') as f_out:
                    f_out.writelines(f_in)
                file_count += 1 #--- counting files created

            #shutil.copy(copy_to_ldm_dir + new_filename, final_dir + new_filename)
            os.remove(filepath) #--- remove files from processing directory

    #--- logging files created for date
    pattern = os.path.join(base.final_dir, f"*{base.file_dt.strftime('%Y%m%d')}*.nc.gz")
    file_count_total = len(glob.glob(pattern))
    logging.info(f"Created {file_count} AWIPS files. Total for {base.file_dt.strftime('%Y-%m-%d')} is now {file_count_total}.")
        
    return
    
#-----------------------------------------------------

def removeTempFiles(orbit_file):
    # Leave the directory behind if files we don't expect exist (e.g. we didn't finish copy .nc files out)
    num_h5_files = len(glob.glob(orbit_file.raw_files_dir + '/*.h5'))
    num_all_files = len(glob.glob(orbit_file.raw_files_dir + '/*'))
    if (num_all_files - num_h5_files) == 0:
        shutil.rmtree(orbit_file.raw_files_dir)

    if len(glob.glob(orbit_file.processing_dir)) == 1: #--- only log file is left
        shutil.rmtree(orbit_file.processing_dir)

    return

#-----------------------------------------------------

def finishAndClean(base):
    #--- if directory is empty, delete it
    if not os.listdir(base.dtstamp_dir):
        shutil.rmtree(base.dtstamp_dir)

    logging.info(base.log_prefix + 'Finished at ' + datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
    return

#=====================================================

if __name__ == '__main__':
    main()
