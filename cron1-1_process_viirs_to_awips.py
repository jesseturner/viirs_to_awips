#------ run on polarbear3 with python3
#------ rename to make it more clear this is the main processing code

from datetime import datetime
import argparse, glob, gzip, logging, os, shutil, subprocess, re, sys
from dataclasses import dataclass
from pprint import pprint

#=====================================================

@dataclass #--- Config settings that remain constant
class BaseState:
    base_dir: str = None
    sats_to_process: list[str] = None
    bands_to_process: list[str] = None
    orbits_to_process: list[str] = None
    current_dt: datetime = None
    file_dt: datetime = None
    log_prefix: str = None
    dtstamp_dir: str = None
    final_dir: str = None
    band_params: dict[str, str] = None
    raw_sat_names: dict[str, str] = None

@dataclass #--- Changing each orbit
class IterState:
    filepaths: list[str] = None
    raw_files_dir: str = None
    processing_dir: str = None
    missing_p2g_tags: list[str] = None

#=====================================================

def main(raw_args=None):

    base = BaseState()
    setUpVariables(base)
    startLogging(base)
    parseArguments(raw_args, base)
    createTempAndOutputDir(base)
    setSatellitesAndBands(base)
    getOrbits(base)
    pprint(base)

    for sat in base.sats_to_process:
        for band in base.bands_to_process:
            for orbit in base.orbits_to_process:
                iter_state = IterState()

                gettingFilesFromOrbit(base, iter_state, sat, band, orbit)
                grabbingViirsFiles(base, iter_state, sat, band, orbit)
                runningPolar2Grid(base, iter_state, sat, band, orbit)
                checkForMissingData(base, iter_state, sat, band, orbit)
                nameAndFillFiles(base, iter_state, sat, band, orbit)
                removeTempFiles(iter_state)

                pprint(iter_state)

    finishAndClean(base)
    
#=====================================================

def setUpVariables(base: BaseState):

    base.base_dir = os.getcwd()
    base.bands_to_process = ['m', 'i']
    base.sats_to_process = ['NPP', 'J01', 'J02']
    base.current_dt = datetime.now() #datetime(2025, 7, 1, 10, 33, 33)
    base.file_dt = datetime.now() #datetime(2025, 7, 1, 10, 33, 33)

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
    parser = argparse.ArgumentParser(description='Process incoming data from /mnt/viirs/WI-CONUS/NPP for AWIPS ingestion')
    parser.add_argument('-b', '--orbit', type=str,
                        help='Ignore the time search and only process files for orbit b (can omit the leading b)')
    parser.add_argument('-f', '--freq-band', type=str,
                        help='Only process the files for the indicated band groupings (valid inputs are "i" and "m"')
    parser.add_argument('-s', '--satellite', type=str,
                        help='Only process the selected satellite ("NPP", "J01", and "J02")')
    parser.add_argument('-d', '--file-date', type=str,
                        help='Run for the full 24 hours of a specific date (YYYYMMDD format) or for a specific hour (YYYYMMDDhh format)')
    
    args = parser.parse_args(raw_args)

    #--- No arguments
    if len(sys.argv) == 1:
        logging.info(f"{base.log_prefix} Looking for data from {base.file_dt.strftime('%Y-%m-%d %H')}:00 UTC")

    #--- Specified orbit
    if args.orbit:
        base.orbits_to_process = [args.orbit if args.orbit[0] == 'b' else ('b' + args.orbit)]
        logging.info(f"{base.log_prefix} Running for orbit {base.orbits_to_process}")

    #--- Specified band
    if args.freq_band:
        if args.freq_band not in base.bands_to_process:
            logging.error('Invalid single-band input; valid options are "i" and "m"')
            exit(1)
        base.bands_to_process = [args.freq_band]

    #--- Specified satellite
    if args.satellite:
        if args.satellite not in base.sats_to_process:
            logging.error('Invalid satellite input; valid options are "NPP", "J01", and "J02"')
            exit(1)
        base.sats_to_process = [args.satellite]

    #--- Specified date
    if args.file_date:
        #--- Depending on if hour is specified or not
        if len(args.file_date) == 10:  #--- format: YYYYMMDDhh
            base.file_dt = datetime.strptime(args.file_date, "%Y%m%d%H")
            logging.info(f"{base.log_prefix} Looking for data from {base.file_dt.strftime('%Y-%m-%d %H')}:00 UTC")
        elif len(args.file_date) == 8:  #--- format: YYYYMMDD
            base.file_dt = datetime.strptime(args.file_date, "%Y%m%d")
            logging.info(f"{base.log_prefix} Looking for all data from {base.file_dt.strftime('%Y-%m-%d')}")
        else:
            raise ValueError("file_date must be in YYYYMMDD or YYYYMMDDhh format")
        
    
    return

#-----------------------------------------------------

def getOrbits(base: BaseState):

    if not base.orbits_to_process: #--- initialize, if no argument for orbits
        base.orbits_to_process = []
    
    for sat in base.sats_to_process:    #--- NPP, J01, J02
        for band in base.bands_to_process:   #--- m, i
            
            #--- set band parameters
            band_dir = base.band_params[band]['band_dir'].replace('_replacewithsat_', sat)

            #--- get files that match time
            file_date_str = f"d{base.file_dt.year}{base.file_dt.month:02d}{base.file_dt.day:02d}_t{base.file_dt.hour:02d}"
            matching_files = [
                os.path.basename(f) for f in glob.glob(os.path.join(band_dir, f"*{file_date_str}*"))
                if file_date_str in f
            ]
            if not matching_files:
                continue

            #--- create orbits list from matching files
            for filename in matching_files:  
                orbit = filename.split('_')[5]
                if orbit not in base.orbits_to_process:
                    base.orbits_to_process.append(orbit)
    
    return

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
            'band_dir': f"/mnt/viirs/WI-CONUS/_replacewithsat_/SDR-MBand/{base.file_dt.year}/{base.file_dt.timetuple().tm_yday}/",
            'prod_prefixes': ['GMTCO'] + ['SV' + tag for tag in list(m_ldm_file_tags.values())],
            'ldm_file_tags': m_ldm_file_tags,
            'output_prod_name': 'VIIRS'
        },
        'i': {
            'band_dir': f"/mnt/viirs/WI-CONUS/_replacewithsat_/SDR-IBand/{base.file_dt.year}/{base.file_dt.timetuple().tm_yday}/",
            'prod_prefixes': ['GITCO'] + ['SV' + tag for tag in list(i_ldm_file_tags.values())],
            'ldm_file_tags': i_ldm_file_tags,
            'output_prod_name': 'VIIRS'
        }
    }

    base.raw_sat_names = {'NPP': 'npp', 'J01': 'noaa20' , 'J02': 'noaa21'}

    return

#-----------------------------------------------------

def gettingFilesFromOrbit(base, iter_state, sat, band, orbit):

    iter_state.filepaths = [] #--- Initialize

    prod_prefixes = base.band_params[band]['prod_prefixes']

    #--- create list of recent orbit files
    band_dir = base.band_params[band]['band_dir'].replace('_replacewithsat_', sat)
    orbit_files_recent = [f for f in os.listdir(band_dir) if
                        f[0:5] in prod_prefixes and '_' + orbit + '_' in f]
    
    #--- count number of orbit files, warning if one of the bands has a different number
    #------ I bet there is a simpler way of doing this
    num_files = len([f for f in orbit_files_recent if f[0:5] == prod_prefixes[0]])
    orbit_not_ready = False

    for prefix in prod_prefixes:
        if num_files != len([f for f in orbit_files_recent if f[0:5] == prefix]):
            orbit_not_ready = True
            break
        if orbit_not_ready:
            logging.info(f"{sat} orbit {orbit} bands are not fully filled in yet, not processing")
            continue
        
        #--- logging the files used
        iter_state.filepaths = glob.glob(os.path.join(band_dir, prefix + '*_' + orbit + '_*'))
        if not iter_state.filepaths:
            continue
        else: 
            iter_state.filepaths.sort()  #--- sort to follow time order
            first_file = os.path.basename(iter_state.filepaths[0])
            match = re.search(r'd(\d{8})_t(\d{2})(\d{2})', first_file)
            if match:
                date_str, hour, minute = match.groups()
                dt = datetime.strptime(f"{date_str}{hour}{minute}", "%Y%m%d%H%M")
                datetime_str = dt.strftime("%Y-%m-%d %H:%M UTC")
            else:
                datetime_str = "Unknown datetime"
                
    if iter_state.filepaths:
        logging.info(f"Processing {len(iter_state.filepaths)} VIIRS files for {sat} orbit {orbit} {band}-band at {datetime_str}")  
    
    return
    
#-----------------------------------------------------

def grabbingViirsFiles(base, iter_state, sat, band, orbit):

    iter_state.processing_dir = base.dtstamp_dir + sat + '_' + band + '_' + orbit + '/'
    os.makedirs(iter_state.processing_dir)
    iter_state.raw_files_dir = iter_state.processing_dir + 'raw_files/'
    os.makedirs(iter_state.raw_files_dir)

    for filepath in iter_state.filepaths:
        shutil.copy(filepath, iter_state.raw_files_dir) #--- copying the file
            
    return
    
#-----------------------------------------------------

def runningPolar2Grid(base, iter_state, sat, band, orbit):
    #--- running the polar2grid package
    #------ this is the very core of the processing
        
    print(base.log_prefix + 'Running p2g for ' + sat + ' ' + band + ' band ')
    if os.listdir(iter_state.raw_files_dir):  #--- checks if directory is not empty
        p2g_status = subprocess.call(
            ['bash', os.path.join(base.base_dir, f'call_p2g_{band}.sh'), iter_state.raw_files_dir],
            cwd=iter_state.processing_dir
        )

        print(f"{base.log_prefix} Finished running p2g for {sat} (orbit {orbit}) {band} band with exit status {p2g_status}")
                
    return
    
#-----------------------------------------------------

def checkForMissingData(base, iter_state, sat, band, orbit):
    
    raw_sat_name = base.raw_sat_names[sat]

    #--- check if channels are missing
    #------ there is definitely a simpler way of doing this
    
    ldm_file_tags = base.band_params[band]['ldm_file_tags']
    p2g_file_tags = list(ldm_file_tags.keys())

    iter_state.missing_p2g_tags = []
    for p2g_tag in p2g_file_tags:
        if len(glob.glob(iter_state.processing_dir + 'SSEC_AII_' + raw_sat_name + '_viirs_' + p2g_tag + '*.nc')) == 0:
            iter_state.missing_p2g_tags.append(p2g_tag)
    #--- all files are missing tags, likely due to <10% grid coverage
    if len(iter_state.missing_p2g_tags) == len(p2g_file_tags):
        logging.info(f"P2G returned no files for {sat} orbit {orbit} {band}-band at {base.file_dt.strftime('%Y-%m-%d %H:%M UTC')}")

    #--- some files are missing tags, likely due to lack of sun
    elif len(iter_state.missing_p2g_tags) > 0:
        logging.info(f"P2G rejected {iter_state.missing_p2g_tags} for {sat} orbit {orbit} {band}-band at {base.file_dt.strftime('%Y-%m-%d %H:%M UTC')}")
    
    return

#-----------------------------------------------------

def nameAndFillFiles(base, iter_state, sat, band, orbit):
    #--- for each file type, names properly and fills with gzip-compressed data
    
    raw_sat_name = base.raw_sat_names[sat]

    ldm_file_tags = base.band_params[band]['ldm_file_tags']
    p2g_file_tags = list(ldm_file_tags.keys())
    output_prod_name = base.band_params[band]['output_prod_name']

    file_count = 0
    for p2g_tag in p2g_file_tags:
        for filepath in glob.glob(
                iter_state.processing_dir + 'SSEC_AII_' + raw_sat_name + '_viirs_' + p2g_tag + '*.nc'):
            filename = os.path.basename(filepath)
            filename_pieces = filename.split('_')

            # polar2grid default is SSEC_AII_[raw_sat_name]_viirs_m##_LCC_Tttt_yyyymmdd_hhmm.nc
            # desired output is RAMMB_ppppp_bbb_yyyymmdd_hhmm_ttt.nc.gz
            # (ppppp = VIIRS, others later?), (bbb = band, e.g. M08, I03)
            new_filename = ('RAMMB_' + output_prod_name + '_' + ldm_file_tags[p2g_tag] + '_' +
                            filename_pieces[7] + '_' + filename_pieces[8][:-3] + '_' +
                            filename_pieces[6][1:] + '.nc.gz')

            if p2g_tag not in iter_state.missing_p2g_tags:
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

def removeTempFiles(iter_state):
    # Leave the directory behind if files we don't expect exist (e.g. we didn't finish copy .nc files out)
    num_h5_files = len(glob.glob(iter_state.raw_files_dir + '/*.h5'))
    num_all_files = len(glob.glob(iter_state.raw_files_dir + '/*'))
    if (num_all_files - num_h5_files) == 0:
        shutil.rmtree(iter_state.raw_files_dir)

    if len(glob.glob(iter_state.processing_dir)) == 1: #--- only log file is left
        shutil.rmtree(iter_state.processing_dir)

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
